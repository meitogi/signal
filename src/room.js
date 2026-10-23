// joinRoom() : the room, the mesh and the handshakes. Platform-free : `createClient(platform)` gets
// `RTCPeerConnection` and `WebSocket` from the browser entry (index.js) or the Node one (node.js).
//
// Who talks to whom : the newcomer offers to every id its `welcome` lists (but itself and the peers it
// is already directly linked to) ; members never offer, so two peers never offer each other (no glare).
// A handshake : sealed offer → sealed answer carrying the answerer's proof → the offerer verifies it,
// applies the answer → the DataChannel opens → the offerer sends its proof on it → the answerer
// verifies and replies `ok`. `peer` is emitted on `ok` (sent / received) and only then. Before that,
// each side reads exactly one text message of at most PRE_PROOF_MAX bytes of the expected shape ;
// anything else closes the connection without being parsed.
//
// Fallback through the relay (`fallback: true`) : when the direct path fails on the offerer's side it
// sends a sealed `data` frame `join` ; the target drops any direct attempt with it, answers `ok`, and
// both get a `via: 'relay'` peer. The proof there is the envelope itself : a `data` box only opens
// under the room key, its additional data binds room, sender and recipient, and the per-sender
// counter refuses a replay — a peer that can seal a box knows the secret.
import { deriveRoom, isSecret, mac, randomBytes, b64u, verifyMac } from './crypto.js';
import { Emitter } from './emitter.js';
import { SignalError } from './errors.js';
import { directPeer, createPeer } from './peer.js';
import { createRelaySession } from './relay-session.js';
import { capMessageSize, transcript } from './sdp.js';

export const STUN = [{ urls: 'stun:stun.l.google.com:19302' }];
/** Largest pre-proof message read on a channel ; above it, the peer is cut unread. */
const PRE_PROOF_MAX = 1024;
/** Inbound handshakes allowed at once : a stranger with the room id cannot make us open more. */
const INBOUND_MAX = 4;
/** Offers we send one peer per minute : half the relay's RELAY_OFFERS_PER_MIN (6), so 4429 never comes. */
const OFFERS_PER_MIN = 3;
const OFFER_WINDOW_MS = 60_000;
/** Payload per relay frame : the JSON `data` message (base64 box, 64-char ids) stays under RELAY_MAX_BYTES (16 384). */
const RELAY_CHUNK = 11 * 1024;
const TAG = { msg: 0, join: 1, ok: 2, bye: 3 };
const LOCAL_HOSTS = { localhost: true, '127.0.0.1': true };

function validate(o) {
  if (o === null || typeof o !== 'object') return new SignalError('options', 'joinRoom() takes an options object');
  let url = null;
  try { url = new URL(o.relay); } catch { /* invalid */ }
  if (url === null || !(url.protocol === 'wss:' || (url.protocol === 'ws:' && LOCAL_HOSTS[url.hostname]))) {
    return new SignalError('options', '`relay` must be a wss:// URL (ws:// only on localhost / 127.0.0.1)');
  }
  const room = o.room;
  if (typeof room !== 'string' || room.length < 1 || room.length > 64) return new SignalError('options', '`room` must be a string of 1 to 64 characters');
  if (!isSecret(o.secret)) return new SignalError('secret', '`secret` must come from newSecret() : 32 bytes in base64url (43 characters)');
  const max = o.max ?? 8;
  if (!Number.isInteger(max) || max < 2) return new SignalError('options', '`max` must be an integer ≥ 2');
  if (o.relayKey !== undefined && typeof o.relayKey !== 'string') return new SignalError('options', '`relayKey` must be a string');
  if (o.iceServers !== undefined && !Array.isArray(o.iceServers)) return new SignalError('options', '`iceServers` must be an array');
  for (const name of ['connectMs', 'handshakeMs', 'maxMessageBytes']) {
    const v = o[name];
    if (v !== undefined && !(Number.isInteger(v) && v > 0)) return new SignalError('options', `\`${name}\` must be a positive integer`);
  }
  return null;
}

/**
 * @param {{RTCPeerConnection: typeof RTCPeerConnection, WebSocket: typeof WebSocket}} platform
 * @returns {{joinRoom(options: object): object}} see CONTRACT.md « Options de joinRoom() » and « Room »
 */
export function createClient(platform) {
  const { RTCPeerConnection, WebSocket } = platform;

  function joinRoom(options) {
    const room = new Emitter();
    let resolveReady;
    let rejectReady;
    room.ready = new Promise((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
    room.ready.catch(() => {}); // an app that only listens to `close` gets no unhandled rejection
    room.id = null;
    room.isCreator = false;
    room.max = null;

    /** id → ready peer (direct or relay) */
    const peers = Object.create(null);
    /** id → handshake in progress */
    const handshakes = Object.create(null);
    /** id → pending relay join we initiated (timer) */
    const relayJoins = Object.create(null);
    /** target id → timestamps of our offers */
    const offers = Object.create(null);
    let inbound = 0;
    let session = null;
    let ended = false;
    let entered = false;

    const peerList = () => {
      const list = [];
      for (const id in peers) list.push(peers[id]);
      return list;
    };
    Object.defineProperty(room, 'peers', { get: peerList, enumerable: true });

    const incident = (code, message) => room.emit('error', new SignalError(code, message, false));

    const bad = validate(options);
    if (bad !== null) {
      setTimeout(() => end(bad), 0);
      return room;
    }
    const {
      relay, room: roomName, secret, relayKey, iceServers = STUN, fallback = true,
      connectMs = 20_000, handshakeMs = 10_000, maxMessageBytes = 1_048_576, limits,
    } = options;
    const max = options.max ?? 8;
    let roomId = null;
    let keys = null;

    // ── peers ──────────────────────────────────────────────────────────────────────────────────

    function adopt(peer) {
      peers[peer.id] = peer;
      peer.on('close', (reason) => {
        if (peers[peer.id] !== peer) return;
        delete peers[peer.id];
        if (reason === 'peer-rate' || reason === 'too-big' || reason === 'malformed') incident(reason === 'malformed' ? 'proof' : reason, `peer ${peer.id} cut : ${reason}`);
        room.emit('leave', peer, reason);
      });
      room.emit('peer', peer);
    }

    function relayPeer(id) {
      const peer = createPeer({
        id,
        via: 'relay',
        maxMessageBytes,
        limits,
        chunk: RELAY_CHUNK,
        sendFrame: (frame) => {
          const out = new Uint8Array(1 + frame.length);
          out[0] = TAG.msg;
          out.set(frame, 1);
          return session.data(id, out);
        },
        shutdown: () => { if (!ended) session.data(id, new Uint8Array([TAG.bye])); },
      });
      adopt(peer);
      return peer;
    }

    /** How many peers we hold or are opening ; the creator keeps it under `max`. */
    function occupancy() {
      let n = 1;
      for (const _ in peers) n++;
      for (const _ in handshakes) n++;
      for (const _ in relayJoins) n++;
      return n;
    }
    const roomHasRoom = () => !room.isCreator || occupancy() < room.max;

    // ── handshakes ─────────────────────────────────────────────────────────────────────────────

    function finish(h) {
      if (h.done) return;
      h.done = true;
      clearTimeout(h.connectTimer);
      clearTimeout(h.handshakeTimer);
      if (handshakes[h.id] === h) delete handshakes[h.id];
      if (h.role === 'answerer') inbound--;
    }

    /** A handshake that did not make it. `code` null = silent (we gave it up ourselves). */
    function fail(h, code, message) {
      if (h.done) return;
      finish(h);
      h.pc.close();
      if (code === null) return;
      const direct = code === 'ice';
      if (direct && h.role === 'offerer' && fallback) return joinViaRelay(h.id);
      if (direct && !fallback) return incident('ice', message);
      if (direct) return; // answerer with fallback : the offerer's `join` will come through the relay
      incident(code, message);
    }

    function gate(h, channel, expect) {
      h.channel = channel;
      channel.binaryType = 'arraybuffer';
      const first = ({ data }) => {
        channel.removeEventListener('message', first);
        // Reject before read : size and type are checked on the raw frame, nothing is parsed.
        if (typeof data !== 'string' || data.length > PRE_PROOF_MAX) return fail(h, 'proof', `peer ${h.id} sent ${typeof data === 'string' ? data.length + ' chars' : 'binary'} before its proof : cut unread`);
        let msg = null;
        try { msg = JSON.parse(data); } catch { /* not JSON */ }
        if (msg === null || typeof msg !== 'object') return fail(h, 'proof', `peer ${h.id} sent a non-JSON pre-proof message : cut`);
        expect(msg).catch((e) => fail(h, 'proof', `peer ${h.id} : ${e.message}`));
      };
      channel.addEventListener('message', first);
      channel.addEventListener('close', () => fail(h, 'ice', `channel to ${h.id} closed before the proof`));
      const onOpen = () => {
        clearTimeout(h.connectTimer);
        h.handshakeTimer = setTimeout(() => fail(h, 'timeout', `peer ${h.id} did not complete its proof within ${handshakeMs} ms`), handshakeMs);
        if (h.role === 'offerer') {
          mac(keys.auth, transcript('offerer', h)).then((proof) => {
            if (!h.done) channel.send(JSON.stringify({ t: 'proof', proof }));
          });
        }
      };
      if (channel.readyState === 'open') onOpen();
      else channel.addEventListener('open', onOpen, { once: true });
    }

    function ready(h) {
      if (h.done) return;
      finish(h);
      adopt(directPeer({ id: h.id, pc: h.pc, channel: h.channel, maxMessageBytes, limits }));
    }

    function newHandshake(id, role) {
      const pc = new RTCPeerConnection({ iceServers });
      const h = { id, role, pc, channel: null, offerSdp: null, answerSdp: null, roomId, offerNonce: null, answerNonce: null, remoteSet: false, ice: [], connectTimer: null, handshakeTimer: null, done: false };
      handshakes[id] = h;
      if (role === 'answerer') inbound++;
      h.connectTimer = setTimeout(() => fail(h, 'ice', `no direct path to ${id} within ${connectMs} ms`), connectMs);
      pc.addEventListener('icecandidate', ({ candidate }) => {
        if (!h.done) session.signal(id, 'ice', { kind: 'ice', candidate: candidate ? { candidate: candidate.candidate, sdpMid: candidate.sdpMid, sdpMLineIndex: candidate.sdpMLineIndex } : null });
      });
      pc.addEventListener('connectionstatechange', () => {
        const state = pc.connectionState;
        if (state === 'failed' || state === 'closed') fail(h, 'ice', `no direct path to ${id} (${state})`);
      });
      return h;
    }

    function allowOffer(id) {
      const now = Date.now();
      const stamps = (offers[id] ??= []);
      while (stamps.length && stamps[0] <= now - OFFER_WINDOW_MS) stamps.shift();
      if (stamps.length >= OFFERS_PER_MIN) return false;
      stamps.push(now);
      return true;
    }

    async function offerTo(id) {
      if (!allowOffer(id)) return incident('timeout', `not offering to ${id} again : ${OFFERS_PER_MIN} offers in the last minute`);
      const h = newHandshake(id, 'offerer');
      const pc = h.pc;
      try {
        gate(h, pc.createDataChannel('signal'), async (msg) => {
          if (msg.t !== 'ok') throw new Error('expected ok after our proof');
          ready(h);
        });
        const offer = await pc.createOffer();
        const sdp = capMessageSize(offer.sdp);
        h.offerSdp = sdp;
        h.offerNonce = b64u.encode(randomBytes(16));
        // Queued before setLocalDescription : the candidates it triggers follow the offer on the wire.
        session.signal(id, 'offer', { kind: 'offer', sdp, nonce: h.offerNonce });
        await pc.setLocalDescription({ type: 'offer', sdp });
      } catch (e) {
        fail(h, 'ice', `cannot build an offer for ${id} : ${e.message}`);
      }
    }

    async function onOffer(from, inner) {
      if (handshakes[from] !== undefined) return; // duplicate offer during a handshake : dropped
      if (peers[from] !== undefined) return; // already linked (direct or relay) : a foreign client re-offering is ignored
      if (inbound >= INBOUND_MAX) return incident('timeout', `offer from ${from} dropped : ${INBOUND_MAX} handshakes already in flight`);
      if (!roomHasRoom()) return incident('room-full', `offer from ${from} dropped : room at max ${room.max}`);
      if (typeof inner.sdp !== 'string' || typeof inner.nonce !== 'string') return incident('proof', `malformed offer from ${from} : dropped`);
      const h = newHandshake(from, 'answerer');
      const pc = h.pc;
      h.offerSdp = inner.sdp;
      h.offerNonce = inner.nonce;
      pc.addEventListener('datachannel', ({ channel }) => {
        if (h.channel !== null) return;
        gate(h, channel, async (msg) => {
          if (msg.t !== 'proof' || !(await verifyMac(keys.auth, transcript('offerer', h), msg.proof))) throw new Error('did not prove the room secret over these DTLS fingerprints');
          if (h.done) return;
          channel.send(JSON.stringify({ t: 'ok' }));
          ready(h);
        });
      });
      try {
        await pc.setRemoteDescription({ type: 'offer', sdp: inner.sdp });
        h.remoteSet = true;
        for (const c of h.ice) pc.addIceCandidate(c).catch(() => {});
        h.ice = [];
        const answer = await pc.createAnswer();
        const sdp = capMessageSize(answer.sdp);
        h.answerSdp = sdp;
        h.answerNonce = b64u.encode(randomBytes(16));
        const proof = await mac(keys.auth, transcript('answerer', h));
        if (h.done) return;
        session.signal(from, 'answer', { kind: 'answer', sdp, nonce: h.answerNonce, proof });
        await pc.setLocalDescription({ type: 'answer', sdp });
      } catch (e) {
        fail(h, 'proof', `cannot answer ${from} : ${e.message}`);
      }
    }

    async function onAnswer(from, inner) {
      const h = handshakes[from];
      if (h === undefined || h.role !== 'offerer' || h.answerSdp !== null) return;
      if (typeof inner.sdp !== 'string' || typeof inner.nonce !== 'string') return fail(h, 'proof', `malformed answer from ${from}`);
      h.answerSdp = inner.sdp;
      h.answerNonce = inner.nonce;
      if (!(await verifyMac(keys.auth, transcript('answerer', h), inner.proof))) return fail(h, 'proof', `peer ${from} did not prove the room secret over these DTLS fingerprints`);
      if (h.done) return;
      try {
        await h.pc.setRemoteDescription({ type: 'answer', sdp: inner.sdp });
        h.remoteSet = true;
        for (const c of h.ice) h.pc.addIceCandidate(c).catch(() => {});
        h.ice = [];
      } catch (e) {
        fail(h, 'proof', `cannot apply the answer of ${from} : ${e.message}`);
      }
    }

    function onIce(from, inner) {
      const h = handshakes[from];
      if (h === undefined) return;
      const c = inner.candidate;
      if (c !== null && (c === undefined || typeof c !== 'object' || typeof c.candidate !== 'string')) return;
      if (!h.remoteSet) return void h.ice.push(c);
      h.pc.addIceCandidate(c).catch(() => {});
    }

    // ── relay fallback ─────────────────────────────────────────────────────────────────────────

    function joinViaRelay(id) {
      if (relayJoins[id] !== undefined || peers[id] !== undefined) return;
      relayJoins[id] = setTimeout(() => {
        delete relayJoins[id];
        incident('timeout', `peer ${id} did not accept the relay fallback within ${handshakeMs} ms`);
      }, handshakeMs);
      session.data(id, new Uint8Array([TAG.join]));
    }

    function onData(from, bytes) {
      const tag = bytes[0];
      const peer = peers[from];
      if (tag === TAG.msg) {
        if (peer !== undefined && peer.via === 'relay') peer.receive(bytes.subarray(1));
      } else if (tag === TAG.join) {
        if (!fallback) return;
        if (peer !== undefined) {
          if (peer.via === 'relay') session.data(from, new Uint8Array([TAG.ok])); // our ok was lost : again
          return;
        }
        if (!roomHasRoom()) return incident('room-full', `relay join from ${from} dropped : room at max ${room.max}`);
        const h = handshakes[from];
        if (h !== undefined) fail(h, null); // the offerer gave up on the direct path
        session.data(from, new Uint8Array([TAG.ok]));
        relayPeer(from);
      } else if (tag === TAG.ok) {
        const timer = relayJoins[from];
        if (timer === undefined) return;
        clearTimeout(timer);
        delete relayJoins[from];
        relayPeer(from);
      } else if (tag === TAG.bye) {
        if (peer !== undefined && peer.via === 'relay') peer.end('closed');
      }
    }

    // ── room life ──────────────────────────────────────────────────────────────────────────────

    function dropRelayPeers(reason) {
      for (const id in peers) if (peers[id].via === 'relay') peers[id].end(reason);
      for (const id in relayJoins) { clearTimeout(relayJoins[id]); delete relayJoins[id]; }
    }

    function onWelcome({ peers: list, max: roomMax, creator, first }) {
      room.id = session.id;
      room.isCreator = creator;
      room.max = roomMax;
      if (!first) dropRelayPeers('relay'); // the other side saw us leave and dropped us too
      const self = session.id;
      for (const { id } of list) {
        if (typeof id !== 'string' || id === self || peers[id] !== undefined || handshakes[id] !== undefined) continue;
        offerTo(id);
      }
      if (!entered) {
        entered = true;
        resolveReady();
      }
    }

    function onPeers(list) {
      const present = Object.create(null);
      for (const p of list) present[p.id] = true;
      for (const id in peers) if (peers[id].via === 'relay' && !present[id]) peers[id].end('relay');
      for (const id in relayJoins) if (!present[id]) { clearTimeout(relayJoins[id]); delete relayJoins[id]; }
    }

    function end(error) {
      if (ended) return;
      ended = true;
      for (const id in handshakes) fail(handshakes[id], null);
      for (const id in relayJoins) clearTimeout(relayJoins[id]);
      for (const id in peers) peers[id].end('closed');
      if (session !== null) session.stop();
      if (!entered) rejectReady(error ?? new SignalError('closed', 'left before entering the room'));
      room.emit('close', error);
    }

    room.leave = () => end(null);
    room.broadcast = (data) => {
      const list = peerList();
      return Promise.all(list.map((p) => p.send(data).catch((e) => { if (e.code !== 'closed') throw e; }))).then(() => {});
    };

    deriveRoom(secret, roomName).then((derived) => {
      if (ended) return;
      keys = derived;
      roomId = derived.roomId;
      session = createRelaySession({ WebSocket, url: relay, roomId, max, key: relayKey, seal: keys.seal });
      room.id = session.id;
      session.on('welcome', onWelcome);
      session.on('peers', onPeers);
      session.on('signal', ({ from, kind, inner }) => {
        if (kind === 'offer') onOffer(from, inner);
        else if (kind === 'answer') onAnswer(from, inner);
        else onIce(from, inner);
      });
      session.on('data', ({ from, bytes }) => { if (bytes.length > 0) onData(from, bytes); });
      session.on('rejected', ({ from, why }) => incident('proof', `a box from ${from} was dropped (${why}) : sender blocked`));
      session.on('down', (error) => {
        dropRelayPeers('relay');
        for (const id in handshakes) fail(handshakes[id], null); // signaling is gone mid-handshake
        room.emit('error', error);
      });
      session.on('fatal', (error) => end(error));
      session.start();
    });

    return room;
  }

  return { joinRoom };
}
