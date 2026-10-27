// How an app message travels on a channel : `string` / `Uint8Array` / `ArrayBuffer` in, binary
// frames of at most CHUNK + 1 bytes out, whole again on the other side.
//
// Frame : `flags (1) | payload`. flags bit 0 = text (UTF-8), bit 1 = last frame of the message.
// The reliable channel is ordered, so frames need no sequence number — only one message is in flight
// per channel at a time (the sender serialises). The unreliable channel carries whole messages only :
// one frame each, the last-frame bit set (isWhole).
const TEXT = 1;
const LAST = 2;
const utf8 = new TextEncoder();
const decoder = new TextDecoder();

/** Payload bytes per frame : 16 KB, the long-standing interoperable DataChannel message size. */
export const CHUNK = 16 * 1024;

/** True for a frame that is a whole message on its own. */
export const isWhole = (frame) => frame.length >= 1 && (frame[0] & LAST) !== 0;

/**
 * @param {unknown} data - what the app passed to send()
 * @returns {{bytes: Uint8Array, text: boolean}|null} null for anything but string / Uint8Array / ArrayBuffer
 */
export function toBytes(data) {
  if (typeof data === 'string') return { bytes: utf8.encode(data), text: true };
  if (data instanceof Uint8Array) return { bytes: data, text: false };
  if (data instanceof ArrayBuffer) return { bytes: new Uint8Array(data), text: false };
  return null;
}

/** The frames of one message, at least one (an empty message is one frame of one byte). */
export function encodeFrames(bytes, text, chunk = CHUNK) {
  const frames = [];
  const n = bytes.length;
  const base = text ? TEXT : 0;
  let off = 0;
  do {
    const end = Math.min(off + chunk, n);
    const frame = new Uint8Array(1 + end - off);
    frame[0] = base | (end === n ? LAST : 0);
    frame.set(bytes.subarray(off, end), 1);
    frames.push(frame);
    off = end;
  } while (off < n);
  return frames;
}

/**
 * The receiving end : feed it every frame ; `onMessage(data)` fires once per whole message, with a
 * `string` or a `Uint8Array`. Returns `null` when the frame was fine, else the reason to close the
 * peer : `'malformed'` (unknown flags, text flag changing mid-message) or `'too-big'` (over `maxBytes`).
 * @param {number} maxBytes
 * @param {(data: string|Uint8Array) => void} onMessage
 * @returns {(frame: Uint8Array) => null|'malformed'|'too-big'}
 */
export function createAssembler(maxBytes, onMessage) {
  let parts = [];
  let total = 0;
  let text = false;
  return (frame) => {
    if (frame.length < 1) return 'malformed';
    const flags = frame[0];
    if (flags > (TEXT | LAST)) return 'malformed';
    const isText = (flags & TEXT) !== 0;
    if (parts.length === 0) text = isText;
    else if (isText !== text) return 'malformed';
    const payload = frame.subarray(1);
    total += payload.length;
    if (total > maxBytes) return 'too-big';
    parts.push(payload);
    if ((flags & LAST) === 0) return null;
    let bytes;
    if (parts.length === 1) {
      bytes = payload;
    } else {
      bytes = new Uint8Array(total);
      let off = 0;
      for (const p of parts) { bytes.set(p, off); off += p.length; }
    }
    parts = [];
    total = 0;
    onMessage(text ? decoder.decode(bytes) : bytes);
    return null;
  };
}
