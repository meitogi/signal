// Client-side test kit : node peers on the real platform (werift + ws), platforms wrapped to tap the
// wire, meter the channel or break ICE, and a hand-driven "attacker" that knows the secret.
import { strict as assert } from 'node:assert';
import { randomBytes } from 'node:crypto';
import { b64u, createSealer, deriveRoom } from '../src/crypto.js';
import { NodePeerConnection, NodeWebSocket, platform } from '../src/node-platform.js';
import { createRelay } from '../src/relay.js';
import { createClient, UNRELIABLE } from '../src/room.js';
import { join } from './helpers.js';

const utf8 = new TextEncoder();
const decoder = new TextDecoder();

/** A relay on a free port, silent, bans and per-IP caps out of the way. */
export const startRelay = (options) => createRelay({ port: 0, host: '127.0.0.1', log: () => {}, banAfter: 1e6, ipConn: 1000, ...options });
export const urlOf = (relay) => `ws://127.0.0.1:${relay.port}`;

export const nodeJoin = createClient(platform).joinRoom;
export const joinWith = (p) => createClient({ ...platform, ...p }).joinRoom;

/** A platform whose socket records every frame : `log` gets `{dir: 'out'|'in', text}`. */
export function tapped(log, base = platform) {
  class Tap extends base.WebSocket {
    constructor(url) {
      super(url);
      this.on('message', (raw) => log.push({ dir: 'in', text: raw.toString() }));
    }
    send(raw) { log.push({ dir: 'out', text: String(raw) }); super.send(raw); }
  }
  return { ...base, WebSocket: Tap };
}

/** A platform whose peer connections never exchange a candidate : the direct path cannot come up. */
export function brokenIce(base = platform) {
  const strip = (desc) => ({ ...desc, sdp: desc.sdp.split(/\r?\n/).filter((l) => !l.startsWith('a=candidate')).join('\r\n') });
  class Broken extends base.RTCPeerConnection {
    addEventListener(type, fn, options) { if (type !== 'icecandidate') super.addEventListener(type, fn, options); }
    addIceCandidate() { return Promise.resolve(); }
    async createOffer() { return strip(await super.createOffer()); }
    async createAnswer() { return strip(await super.createAnswer()); }
  }
  return { ...base, RTCPeerConnection: Broken };
}

/** A platform that records the highest `bufferedAmount` seen right after a send, in `stats.max`. */
export function metered(stats, base = platform) {
  const meter = (ch) => {
    const send = ch.send.bind(ch);
    ch.send = (d) => { send(d); stats.max = Math.max(stats.max, ch.bufferedAmount); stats.sent++; };
    return ch;
  };
  class Metered extends base.RTCPeerConnection {
    createDataChannel(label, options) { return meter(super.createDataChannel(label, options)); }
    addEventListener(type, fn, options) {
      super.addEventListener(type, type === 'datachannel' ? (e) => fn({ channel: meter(e.channel) }) : fn, options);
    }
  }
  return { ...base, RTCPeerConnection: Metered };
}

/** A platform that records every DataChannel its connections create or receive, as `{how, label, channel}`. */
export function recorded(list, base = platform) {
  class Recorded extends base.RTCPeerConnection {
    createDataChannel(label, options) {
      const channel = super.createDataChannel(label, options);
      list.push({ how: 'created', label, channel });
      return channel;
    }
    addEventListener(type, fn, options) {
      super.addEventListener(type, type === 'datachannel' ? (e) => { list.push({ how: 'received', label: e.channel.label, channel: e.channel }); fn(e); } : fn, options);
    }
  }
  return { ...base, RTCPeerConnection: Recorded };
}

/** A platform whose socket never sends a `ping` : the relay cuts it for idleness (4408). */
export function noPing(base = platform) {
  class Mute extends base.WebSocket {
    send(raw) { if (!String(raw).startsWith('{"type":"ping"')) super.send(raw); }
  }
  return { ...base, WebSocket: Mute };
}

/** Node's platform with a browser-shaped socket : no `httpStatus`, like a real browser. */
export function browserShaped(base = platform) {
  class Blind extends NodeWebSocket {
    constructor(url) { super(url); Object.defineProperty(this, 'httpStatus', { get: () => undefined, set() {} }); }
  }
  return { ...base, WebSocket: Blind };
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Resolves with the arguments of the next `event`, or rejects after `ms`. */
export function once(emitter, event, ms = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { off(); reject(new Error(`no '${event}' within ${ms} ms`)); }, ms);
    const off = emitter.on(event, (...args) => { clearTimeout(timer); off(); resolve(args); });
  });
}

export async function waitFor(check, ms = 8000, what = 'condition') {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await sleep(20);
  }
}

/** True when `event` does not fire within `ms`. */
export const silent = (emitter, event, ms = 400) => once(emitter, event, ms).then(() => false, () => true);

/** Everything `event` emits, as it happens. */
export function collect(emitter, event) {
  const list = [];
  emitter.on(event, (...args) => list.push(args.length === 1 ? args[0] : args));
  return list;
}

/** Joins `n` rooms with `options` and waits for the full mesh. */
export async function meet(options, n = 2, joinRoom = nodeJoin) {
  const rooms = [];
  for (let i = 0; i < n; i++) rooms.push(joinRoom({ iceServers: [], ...options }));
  await Promise.all(rooms.map((r) => r.ready));
  await waitFor(() => rooms.every((r) => r.peers.length === n - 1), 10_000, `a mesh of ${n}`);
  return rooms;
}

export const leaveAll = (rooms) => { for (const r of rooms) r.leave(); };

/** The next message `peer` receives. */
export const nextMessage = (peer, ms = 5000) => once(peer, 'message', ms).then(([data]) => data);

/**
 * A room member driven by hand over a raw socket : it knows the secret, so it can seal anything —
 * valid offers (with a real werift connection behind them), tampered boxes, lies about `kind`.
 */
export async function attacker(relayUrl, { room, secret }, id = `evil-${randomBytes(4).toString('hex')}`) {
  const { roomId, seal, auth } = await deriveRoom(secret, room);
  const sealer = createSealer(seal, roomId, id);
  const ws = await join(relayUrl, { v: 1, id, room: roomId });
  const pcs = [];
  // The counter is taken when sealing starts, the box is ready later : sends go one at a time, in
  // sealing order (as relay-session.js does), or the victim sees a counter go back and blocks us.
  let outbox = Promise.resolve();
  const self = {
    id,
    ws,
    roomId,
    auth,
    sealer,
    /** Seals `inner` for `to` and sends it under the cleartext `kind` ; returns the box bytes. */
    signal(to, kind, inner, { tamper = false } = {}) {
      const sent = outbox.then(async () => {
        const box = await sealer.seal(to, utf8.encode(JSON.stringify(inner)));
        if (tamper) box[box.length - 1] ^= 1;
        ws.send({ type: 'signal', to, kind, box: b64u.encode(box) });
        return box;
      });
      outbox = sent.catch(() => {});
      return sent;
    },
    sendBox(to, kind, box) { ws.send({ type: 'signal', to, kind, box: b64u.encode(box) }); },
    /** The next signal from anyone, opened. */
    async nextSignal(ms = 3000) {
      const msg = await ws.next('signal', ms);
      const plain = await sealer.open(msg.from, b64u.decode(msg.box));
      assert.ok(plain, 'a signal we could not open');
      return { ...msg, inner: JSON.parse(decoder.decode(plain)) };
    },
    /**
     * A real offer to `to` (werift behind it) ; `complete()` applies the answer and candidates until the channel opens.
     * `unreliable` also opens the pre-negotiated unreliable channel, as `o.unreliable`.
     */
    async offer(to, { iceServers = [], unreliable = false } = {}) {
      const pc = new NodePeerConnection({ iceServers });
      pcs.push(pc);
      const channel = pc.createDataChannel('signal');
      const lossy = unreliable ? pc.createDataChannel(UNRELIABLE.label, UNRELIABLE.options) : null;
      const offer = await pc.createOffer();
      const nonce = b64u.encode(randomBytes(16));
      const opened = new Promise((r) => channel.addEventListener('open', r, { once: true }));
      pc.addEventListener('icecandidate', ({ candidate }) => self.signal(to, 'ice', { kind: 'ice', candidate }));
      const box = await self.signal(to, 'offer', { kind: 'offer', sdp: offer.sdp, nonce });
      await pc.setLocalDescription(offer);
      const o = { pc, channel, unreliable: lossy, sdp: offer.sdp, nonce, box, answer: null, opened };
      o.complete = async (ms = 5000) => {
        const end = Date.now() + ms;
        while (o.answer === null || channel.readyState !== 'open') {
          const left = end - Date.now();
          if (left <= 0) throw new Error('channel not open in time');
          const s = await Promise.race([self.nextSignal(left), opened.then(() => null)]);
          if (s === null) break;
          if (s.kind === 'answer' && s.from === to) {
            o.answer = s.inner;
            await pc.setRemoteDescription({ type: 'answer', sdp: s.inner.sdp });
          } else if (s.kind === 'ice' && s.from === to && o.answer !== null) {
            await pc.addIceCandidate(s.inner.candidate).catch(() => {});
          }
        }
        return o;
      };
      return o;
    },
    close() {
      for (const pc of pcs) pc.close();
      ws.close();
      return ws.closed;
    },
  };
  return self;
}
