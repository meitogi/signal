// Keys derived from the room secret, and the envelope that seals everything the relay carries.
// WebCrypto only : the same file runs in the browser and in Node ≥ 22.
//
// From the secret (32 bytes), HKDF-SHA256 with salt = room name derives three things : the room id
// the relay sees (so without the secret there is no room), an AES-GCM-256 key for the envelope,
// an HMAC-SHA256 key for the DTLS identity proof.

const subtle = globalThis.crypto.subtle;
const utf8 = new TextEncoder();
const SECRET_RE = /^[A-Za-z0-9_-]{43}$/;

export const b64u = {
  /** @param {Uint8Array} bytes */
  encode(bytes) {
    let bin = '';
    const n = bytes.length;
    for (let i = 0; i < n; i++) bin += String.fromCharCode(bytes[i]);
    return btoa(bin).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '');
  },
  /** @returns {Uint8Array} throws on characters outside base64url */
  decode(text) {
    if (!/^[A-Za-z0-9_-]*$/.test(text)) throw new Error('not base64url');
    const bin = atob(text.replaceAll('-', '+').replaceAll('_', '/'));
    const n = bin.length;
    const bytes = new Uint8Array(n);
    for (let i = 0; i < n; i++) bytes[i] = bin.charCodeAt(i);
    return bytes;
  },
};

export const randomBytes = (n) => globalThis.crypto.getRandomValues(new Uint8Array(n));

/** A fresh room secret : 32 random bytes, base64url (43 chars). Share it with the peers, never with a server. */
export const newSecret = () => b64u.encode(randomBytes(32));

/** True when `secret` has the shape newSecret() produces. */
export const isSecret = (secret) => typeof secret === 'string' && SECRET_RE.test(secret) && b64u.decode(secret).length === 32;

/** A peer id : 16 random bytes, base64url (22 chars). */
export const newId = () => b64u.encode(randomBytes(16));

/**
 * @param {string} secret - newSecret() output
 * @param {string} room - the app's room name
 * @returns {Promise<{roomId: string, seal: CryptoKey, auth: CryptoKey}>} `roomId` is what the relay sees
 */
export async function deriveRoom(secret, room) {
  const ikm = await subtle.importKey('raw', b64u.decode(secret), 'HKDF', false, ['deriveKey', 'deriveBits']);
  const params = (info) => ({ name: 'HKDF', hash: 'SHA-256', salt: utf8.encode(room), info: utf8.encode(info) });
  const [id, seal, auth] = await Promise.all([
    subtle.deriveBits(params('meitogi-signal room v1'), ikm, 256),
    subtle.deriveKey(params('meitogi-signal seal v1'), ikm, { name: 'AES-GCM', length: 256 }, false, ['encrypt', 'decrypt']),
    subtle.deriveKey(params('meitogi-signal auth v1'), ikm, { name: 'HMAC', hash: 'SHA-256', length: 256 }, false, ['sign', 'verify']),
  ]);
  return { roomId: b64u.encode(new Uint8Array(id)), seal, auth };
}

/**
 * The envelope : `iv (12) | AES-GCM ciphertext + tag`. The IV is 4 random bytes fixed for this
 * sealer, then a 64-bit counter ; the additional data binds room, sender and recipient. So the relay
 * can neither read a message, forge one, redirect it to another peer, nor replay it : `open()`
 * refuses a counter that does not increase for that sender. One sealer per peer id (its counter is
 * shared by everything that id sends) : a receiver pins the first IV prefix it accepts from an id,
 * so a new sealer needs a new id.
 * @param {CryptoKey} seal
 * @param {string} roomId
 * @param {string} selfId
 */
export function createSealer(seal, roomId, selfId) {
  const prefix = randomBytes(4);
  let counter = 0n;
  /** sender id → { prefix (b64u), counter } of the last message accepted */
  const seen = Object.create(null);
  const aad = (from, to) => utf8.encode(`${roomId}\n${from}\n${to}`);

  return {
    /** @param {string} to @param {Uint8Array} bytes @returns {Promise<Uint8Array>} */
    async seal(to, bytes) {
      const iv = new Uint8Array(12);
      iv.set(prefix);
      new DataView(iv.buffer).setBigUint64(4, ++counter);
      const sealed = new Uint8Array(await subtle.encrypt({ name: 'AES-GCM', iv, additionalData: aad(selfId, to) }, seal, bytes));
      const out = new Uint8Array(12 + sealed.length);
      out.set(iv);
      out.set(sealed, 12);
      return out;
    },
    /** @returns {Promise<Uint8Array|null>} null when forged, tampered, misdirected or replayed */
    async open(from, sealed) {
      if (sealed.length < 12 + 16) return null;
      const iv = sealed.subarray(0, 12);
      let plain;
      try {
        plain = new Uint8Array(await subtle.decrypt({ name: 'AES-GCM', iv, additionalData: aad(from, selfId) }, seal, sealed.subarray(12)));
      } catch {
        return null;
      }
      const ivPrefix = b64u.encode(iv.subarray(0, 4));
      const n = new DataView(iv.buffer, iv.byteOffset).getBigUint64(4);
      const last = seen[from];
      if (last && (last.prefix !== ivPrefix || n <= last.counter)) return null;
      seen[from] = { prefix: ivPrefix, counter: n };
      return plain;
    },
  };
}

/** HMAC-SHA256 of `text`, base64url. */
export async function mac(auth, text) {
  return b64u.encode(new Uint8Array(await subtle.sign('HMAC', auth, utf8.encode(text))));
}

/** Constant-time check of a mac() result (WebCrypto verify). */
export async function verifyMac(auth, text, value) {
  let bytes;
  try { bytes = b64u.decode(String(value)); } catch { return false; }
  return subtle.verify('HMAC', auth, bytes, utf8.encode(text));
}
