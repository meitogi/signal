// The socket to the relay, for the life of a room : hello / welcome, pings, reconnection with
// backoff, a new id on 4409, close codes read as SignalErrors (errors.js), and the sealed envelope
// around every `signal` and `data` box. Nothing here knows about WebRTC.
//
// Outgoing messages share one token bucket per session (`sendRate` / `sendBurst`, 100 / 200 by default) :
// a message waits for its token, it is never dropped, and sealing happens in send order so the
// receiver's counter check holds. The relay's `welcome` says what it grants this socket ; the bucket
// then stays at GRANT_SHARE of it, so the relay never drops our frames (its own bucket is silent
// before its 4429). Incoming boxes are opened in arrival order for the same reason.
import { b64u, createSealer, newId } from './crypto.js';
import { Emitter } from './emitter.js';
import { closeError, SignalError } from './errors.js';

const PING_MS = 5000;
const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 30_000;
export const SEND_RATE = 100;
export const SEND_BURST = 200;
/** Share of the relay's grant the bucket may use : 100 / 120 and 200 / 240 against its defaults. */
const GRANT_SHARE = 5 / 6;
const SIGNAL_KINDS = { offer: true, answer: true, ice: true };
const utf8 = new TextEncoder();
const decoder = new TextDecoder();

/**
 * @param {object} o
 * @param {typeof WebSocket} o.WebSocket - the platform's socket (Node's may carry `httpStatus`)
 * @param {string} o.url
 * @param {string} o.roomId - the HKDF room id
 * @param {number} o.max
 * @param {string} [o.key] - relayKey
 * @param {number} [o.sendRate=SEND_RATE] - messages per second to the relay, sustained
 * @param {number} [o.sendBurst=SEND_BURST] - messages at once
 * @param {CryptoKey} o.seal
 * @returns {Emitter & {id: string, start(): void, stop(): Promise<void>, signal, data, block}}
 *   events : 'welcome' ({peers, max, creator, first}), 'peers' (list), 'signal' ({from, kind, inner}),
 *   'data' ({from, bytes}), 'rejected' ({from, why}) — a box dropped and its sender blocked,
 *   'down' (SignalError, non-fatal : reconnecting), 'fatal' (SignalError : stopped for good)
 */
export function createRelaySession({ WebSocket, url, roomId, max, key, sendRate = SEND_RATE, sendBurst = SEND_BURST, seal }) {
  const session = new Emitter();
  let id = newId();
  let sealer = createSealer(seal, roomId, id);
  let ws = null;
  let ping = null;
  let retry = null;
  let backoff = BACKOFF_MIN_MS;
  let entered = false; // welcomed at least once in this room's life
  let stopped = false;
  let outbox = Promise.resolve();
  let inbox = Promise.resolve();
  let rate = sendRate;
  let burst = sendBurst;
  let tokens = burst;
  let stamp = Date.now();

  session.id = id;

  const sendRaw = (obj) => {
    if (ws !== null && ws.readyState === 1) ws.send(JSON.stringify(obj));
  };

  /**
   * Resolves once the message may go ; FIFO through `outbox`. The token is taken at once and a deficit
   * is waited off : a timer that fires late is refilled by the next call, so the rate holds under load.
   */
  function acquire() {
    const now = Date.now();
    tokens = Math.min(burst, tokens + ((now - stamp) * rate) / 1000) - 1;
    stamp = now;
    if (tokens >= 0) return Promise.resolve();
    return new Promise((resolve) => setTimeout(resolve, Math.ceil((-tokens * 1000) / rate)));
  }

  const enqueue = (work) => {
    const next = outbox.then(work).catch(() => {});
    outbox = next;
    return next;
  };

  const sendSealed = (to, bytes, frame) => enqueue(async () => {
    const box = b64u.encode(await sealer.seal(to, bytes));
    await acquire();
    sendRaw({ ...frame, to, box });
  });

  /** Our bucket within GRANT_SHARE of what the relay grants ; a relay that says nothing leaves our options. */
  function fitGrant(granted, wanted) {
    return Number.isInteger(granted) && granted > 0 ? Math.min(wanted, granted * GRANT_SHARE) : wanted;
  }

  async function onMessage(msg) {
    if (stopped) return; // queued behind stop() : the room is over, nothing restarts
    const type = msg.type;
    if (type === 'welcome') {
      backoff = BACKOFF_MIN_MS;
      rate = fitGrant(msg.rate, sendRate);
      burst = fitGrant(msg.burst, sendBurst);
      tokens = Math.min(tokens, burst);
      const first = !entered;
      entered = true;
      session.emit('welcome', { peers: msg.peers ?? [], max: msg.max, creator: msg.creator === true, first });
    } else if (type === 'peers') {
      session.emit('peers', msg.list ?? []);
    } else if (type === 'signal' || type === 'data') {
      const from = msg.from;
      if (typeof from !== 'string' || typeof msg.box !== 'string') return;
      let bytes = null;
      try { bytes = await sealer.open(from, b64u.decode(msg.box)); } catch { /* not base64url */ }
      if (bytes === null) return reject(from, 'box');
      if (type === 'data') return session.emit('data', { from, bytes });
      const kind = msg.kind;
      let inner;
      try { inner = JSON.parse(decoder.decode(bytes)); } catch { return reject(from, 'json'); }
      // The cleartext kind lets the relay cap offers ; the sealed one is the truth. They must agree.
      if (inner === null || typeof inner !== 'object' || !SIGNAL_KINDS[kind] || inner.kind !== kind) return reject(from, 'kind');
      session.emit('signal', { from, kind, inner });
    }
  }

  /** A box that is not ours, is replayed or lies about its kind : dropped, sender blocked for us. */
  function reject(from, why) {
    enqueue(async () => { await acquire(); sendRaw({ type: 'block', id: from }); });
    session.emit('rejected', { from, why });
  }

  function connect() {
    let socket;
    try {
      socket = new WebSocket(url);
    } catch (e) {
      return down(new SignalError('relay', `cannot open ${url} : ${e.message}`, false));
    }
    ws = socket;
    socket.addEventListener('open', () => {
      sendRaw({ type: 'hello', v: 1, id, room: roomId, max, key });
      clearInterval(ping);
      ping = setInterval(() => sendRaw({ type: 'ping', ts: Date.now() }), PING_MS);
    });
    socket.addEventListener('message', (event) => {
      let msg;
      try { msg = JSON.parse(event.data); } catch { return; }
      if (msg === null || typeof msg !== 'object') return;
      inbox = inbox.then(() => onMessage(msg)).catch(() => {});
    });
    socket.addEventListener('error', () => {}); // 'close' follows
    socket.addEventListener('close', (event) => {
      if (ws !== socket) return;
      ws = null;
      clearInterval(ping);
      if (stopped) return;
      const error = closeError(event.code, socket.httpStatus, entered);
      if (error.code === 'id-taken') {
        id = newId();
        sealer = createSealer(seal, roomId, id);
        session.id = id;
      }
      if (error.fatal) {
        stopped = true;
        session.emit('fatal', error);
        return;
      }
      down(error);
    });
  }

  function down(error) {
    session.emit('down', error);
    clearTimeout(retry);
    retry = setTimeout(connect, backoff);
    backoff = Math.min(backoff * 2, BACKOFF_MAX_MS);
  }

  session.start = () => { if (!stopped) connect(); };

  /** Flushes what is queued, then closes the socket for good. */
  session.stop = async () => {
    stopped = true;
    clearTimeout(retry);
    await outbox;
    clearInterval(ping);
    if (ws !== null && ws.readyState <= 1) ws.close(1000);
    ws = null;
  };

  /** Sealed `{type:'signal', to, kind, box}` ; `inner.kind` must equal `kind`. */
  session.signal = (to, kind, inner) => sendSealed(to, utf8.encode(JSON.stringify(inner)), { type: 'signal', kind });
  /** Sealed `{type:'data', to, box}`. */
  session.data = (to, bytes) => sendSealed(to, bytes, { type: 'data' });

  return session;
}
