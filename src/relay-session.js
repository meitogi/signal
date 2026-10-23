// The socket to the relay, for the life of a room : hello / welcome, pings, reconnection with
// backoff, a new id on 4409, close codes read as SignalErrors (errors.js), and the sealed envelope
// around every `signal` and `data` box. Nothing here knows about WebRTC.
//
// Outgoing messages share one token bucket (SEND_RATE / SEND_BURST, under the relay's 120 / 240) :
// a message waits for its token, it is never dropped, and sealing happens in send order so the
// receiver's counter check holds. Incoming boxes are opened in arrival order for the same reason.
import { b64u, createSealer, newId } from './crypto.js';
import { Emitter } from './emitter.js';
import { closeError, SignalError } from './errors.js';

const PING_MS = 5000;
const BACKOFF_MIN_MS = 500;
const BACKOFF_MAX_MS = 30_000;
const SEND_RATE = 100;
const SEND_BURST = 200;
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
 * @param {CryptoKey} o.seal
 * @returns {Emitter & {id: string, start(): void, stop(): Promise<void>, signal, data, block}}
 *   events : 'welcome' ({peers, max, creator, first}), 'peers' (list), 'signal' ({from, kind, inner}),
 *   'data' ({from, bytes}), 'rejected' ({from, why}) — a box dropped and its sender blocked,
 *   'down' (SignalError, non-fatal : reconnecting), 'fatal' (SignalError : stopped for good)
 */
export function createRelaySession({ WebSocket, url, roomId, max, key, seal }) {
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
  let tokens = SEND_BURST;
  let stamp = Date.now();

  session.id = id;

  const sendRaw = (obj) => {
    if (ws !== null && ws.readyState === 1) ws.send(JSON.stringify(obj));
  };

  /** Resolves once a token is available ; FIFO through `outbox`. */
  function acquire() {
    const now = Date.now();
    tokens = Math.min(SEND_BURST, tokens + ((now - stamp) * SEND_RATE) / 1000);
    stamp = now;
    if (tokens >= 1) {
      tokens--;
      return Promise.resolve();
    }
    const wait = Math.ceil(((1 - tokens) * 1000) / SEND_RATE);
    return new Promise((resolve) => setTimeout(() => { tokens = 0; stamp = Date.now(); resolve(); }, wait));
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

  async function onMessage(msg) {
    if (stopped) return; // queued behind stop() : the room is over, nothing restarts
    const type = msg.type;
    if (type === 'welcome') {
      backoff = BACKOFF_MIN_MS;
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
