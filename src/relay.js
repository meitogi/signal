// @meitogi/signal relay — WebSocket meeting point for rooms of peers sharing a secret.
//
// A peer says `hello` with a room id (a 43-char HKDF hash of the room secret : the relay never
// sees the secret), then the relay forwards opaque sealed `signal` / `data` boxes between the
// peers of that room, stamping `from` itself. Wire protocol and limits : CONTRACT.md.
//
// Hostile input is refused as early and as cheaply as possible : the connection caps and IP bans
// at the HTTP `upgrade` (429 before any WebSocket), `maxPayload` during reception, the hello
// deadline, a per-socket token bucket, an offer cap per sender→target, an idle deadline.
//
// Access : open, or a list of keys (one per application), each with its own rate. A socket starts
// with the default bucket ; its hello's key switches it to that key's rate, and `welcome` says which.
//
// `RELAY_LEGACY=1` also accepts the old open protocol (hello without `v` / `room`) in room "",
// with `name` / `kind` in the peer lists, as the pre-v1 relay did ; such a hello needs no key.
import { createHash, timingSafeEqual } from 'node:crypto';
import { createServer as createHttpServer } from 'node:http';
import { createServer as createHttpsServer } from 'node:https';
import { WebSocketServer } from 'ws';
import { createGuard, parseTrust } from './ip-guard.js';

/** Close codes sent to a client, in the 4000–4999 application range (1009 is sent by `ws`). */
export const CLOSE = {
  badHello: 4400, // malformed hello
  key: 4401, // missing / wrong relay key, a first message that is not a hello, or no hello in time
  roomFull: 4403,
  idle: 4408, // silent for idleMs after the hello
  idTaken: 4409,
  rate: 4429, // message rate, or offers to one peer
};

/** Our closes that count as a fault toward an IP ban. */
const FAULTS = { [CLOSE.badHello]: true, [CLOSE.key]: true, [CLOSE.rate]: true };
const FIELD_MAX = 64;
const ROOM_ID = /^[A-Za-z0-9_-]{43}$/;
const SIGNAL_KINDS = { offer: true, answer: true, ice: true };
const OFFER_WINDOW_MS = 60_000;
const LEGACY_ROOM = '';
// After our close frame, a peer that never answers it (a silent flooder) is cut this much later,
// instead of holding its slot through `ws`'s 30 s closing handshake.
const CLOSE_GRACE_MS = 1000;

const KEY_NAME = /^[A-Za-z0-9_.-]{1,64}$/;

const digest = (text) => createHash('sha256').update(text).digest();

/**
 * Reads `RELAY_KEYS` : `name:key[:rate[:burst]]` entries, comma-separated. Throws on anything else,
 * naming the entry by position (and name when valid) — never quoting a key.
 * @param {string} text
 * @returns {{name: string, key: string, rate?: number, burst?: number}[]}
 */
export function parseKeys(text) {
  const list = [];
  const names = Object.create(null);
  const digests = Object.create(null);
  const entries = String(text).split(',');
  for (let i = 0; i < entries.length; i++) {
    const fields = entries[i].trim().split(':');
    const at = `RELAY_KEYS entry ${i + 1}`;
    const name = fields[0];
    if (fields.length < 2 || fields.length > 4) throw new Error(`${at} : expected name:key[:rate[:burst]]`);
    if (!KEY_NAME.test(name)) throw new Error(`${at} : the name must be 1 to 64 characters among A-Z a-z 0-9 _ . -`);
    const key = fields[1];
    if (key.length === 0 || /\s/.test(key)) throw new Error(`${at} (${name}) : empty key, or a key with spaces`);
    const [rate, burst] = [fields[2], fields[3]].map((v) => (v === undefined ? undefined : Number(v)));
    for (const n of [rate, burst]) {
      if (n !== undefined && !(Number.isInteger(n) && n > 0)) throw new Error(`${at} (${name}) : rate and burst must be positive integers`);
    }
    if (names[name]) throw new Error(`${at} (${name}) : this name is already used`);
    const d = digest(key).toString('hex');
    if (digests[d]) throw new Error(`${at} (${name}) : same key as ${digests[d]}`);
    names[name] = true;
    digests[d] = name;
    list.push({ name, key, rate, burst });
  }
  return list;
}

function validField(value, required) {
  if (value === undefined && !required) return true;
  return typeof value === 'string' && value.length <= FIELD_MAX && (!required || value.length > 0);
}

/**
 * Starts a relay and resolves once it listens.
 * @param {object} [options]
 * @param {number} [options.port=8787] - 0 picks a free port (tests)
 * @param {string} [options.host='0.0.0.0']
 * @param {string} [options.key=''] - RELAY_KEY : a list of one key named `default`, at `rate` / `burst`
 * @param {{name: string, key: string, rate?: number, burst?: number}[]} [options.keys] - RELAY_KEYS (parseKeys) ;
 *   an entry without rate / burst gets `rate` / `burst`. Neither `key` nor `keys` = open relay ; both = refused
 * @param {{cert: string|Buffer, key: string|Buffer}} [options.tls] - serve wss:// itself
 * @param {number} [options.roomMax=8] - ceiling of a room's `max`
 * @param {number} [options.maxBytes=16384] - largest WebSocket message accepted (1009 above)
 * @param {number} [options.helloMs=5000] - a socket without a valid hello by then is closed 4401
 * @param {number} [options.idleMs=30000] - a v1 socket silent this long after its hello is closed 4408
 * @param {number} [options.rate=120] - messages per second per socket, sustained : before the hello, on an open
 *   relay, for legacy hellos, and for a key without its own
 * @param {number} [options.burst=240] - messages a socket may send at once (same cases)
 * @param {number} [options.offersPerMin=6] - offers from one peer to the same peer per minute
 * @param {number} [options.ipConn=10] - concurrent sockets per IP
 * @param {number} [options.pendingMax=200] - sockets without a hello, all IPs
 * @param {number} [options.connMax=2000] - sockets, all IPs
 * @param {number} [options.banAfter=5] - faults within 10 min before an IP ban
 * @param {number} [options.banMs=600000] - first ban ; doubled on each repeat
 * @param {string} [options.trustProxy=''] - IPs / CIDRs whose X-Forwarded-For gives the client IP
 * @param {boolean} [options.legacy=false] - also accept the old protocol, in room ""
 * @param {number} [options.heartbeatMs=30000] - WebSocket ping period ; a socket that missed one is terminated
 * @param {boolean} [options.logData=false] - log the head of each forwarded `data` box (ciphertext)
 * @param {(line: string) => void} [options.log=console.log]
 * @returns {Promise<{port: number, setTls(tls: {cert, key}): void, stats(): object, close(): Promise<void>}>}
 *   `setTls` swaps the certificate for new TLS handshakes ; open sockets keep theirs.
 */
export function createRelay({
  port = 8787,
  host = '0.0.0.0',
  key = '',
  keys,
  tls,
  roomMax = 8,
  maxBytes = 16384,
  helloMs = 5000,
  idleMs = 30_000,
  rate = 120,
  burst = 240,
  offersPerMin = 6,
  ipConn = 10,
  pendingMax = 200,
  connMax = 2000,
  banAfter = 5,
  banMs = 600_000,
  trustProxy = '',
  legacy = false,
  heartbeatMs = 30_000,
  logData = false,
  log = console.log,
} = {}) {
  if (key && keys !== undefined) return Promise.reject(new Error('give `key` or `keys`, not both (RELAY_KEY or RELAY_KEYS)'));
  /** One grant per key : who gets in, at which rate. Empty = open relay. */
  const grants = (keys ?? (key ? [{ name: 'default', key }] : [])).map((k) => ({
    name: k.name, digest: digest(k.key), rate: k.rate ?? rate, burst: k.burst ?? burst,
  }));
  const guard = createGuard({ ipConn, pendingMax, connMax, banAfter, banMs, trust: parseTrust(trustProxy), log });
  /** room id → {max, size, legacy, peers: peer id → client} */
  const rooms = Object.create(null);
  let roomCount = 0;

  const onRequest = (req, res) => {
    const ok = req.url === '/healthz';
    res.writeHead(ok ? 200 : 426, { 'content-type': 'text/plain' });
    res.end(ok ? 'ok\n' : '@meitogi/signal relay : WebSocket only\n');
  };
  // Sockets that never finish their request headers are dropped after the hello delay too.
  const httpOptions = { headersTimeout: helloMs, connectionsCheckingInterval: Math.min(helloMs, 30_000) };
  const server = tls
    ? createHttpsServer({ ...httpOptions, cert: tls.cert, key: tls.key }, onRequest)
    : createHttpServer(httpOptions, onRequest);
  const wss = new WebSocketServer({ noServer: true, maxPayload: maxBytes, perMessageDeflate: false });

  server.on('upgrade', (req, socket, head) => {
    socket.on('error', () => {});
    const ip = guard.ipOf(req);
    const slot = guard.admit(ip);
    if (typeof slot === 'string') {
      // Before the WebSocket exists : a refusal costs one write, no state is kept for the socket.
      socket.once('finish', () => socket.destroy());
      socket.end('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      return;
    }
    socket.once('close', () => guard.release(slot));
    wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws, req, slot));
  });

  function peerList(room) {
    const list = [];
    const peers = room.peers;
    for (const id in peers) {
      if (!room.legacy) {
        list.push({ id });
        continue;
      }
      const c = peers[id];
      list.push({ id, name: c.name, kind: c.kind });
    }
    return list;
  }

  function broadcastPeers(room) {
    const raw = JSON.stringify({ type: 'peers', list: peerList(room) });
    const peers = room.peers;
    for (const id in peers) peers[id].send(raw);
  }

  /** The grant whose key `candidate` is, or null. Every entry is compared, match or not. */
  function grantOf(candidate) {
    if (typeof candidate !== 'string') return null;
    const d = digest(candidate);
    let found = null;
    for (const g of grants) if (timingSafeEqual(d, g.digest)) found = g;
    return found;
  }

  function shut(client, code, reason) {
    if (client.killTimer !== null) return; // already closing : one close, one fault
    clearTimeout(client.helloTimer);
    clearTimeout(client.idleTimer);
    if (FAULTS[code]) guard.fault(client.slot.ip);
    const ws = client.ws;
    ws.close(code, reason);
    client.killTimer = setTimeout(() => ws.terminate(), CLOSE_GRACE_MS);
  }

  function join(client, msg, isLegacy, grant) {
    const id = msg.id;
    const roomId = isLegacy ? LEGACY_ROOM : msg.room;
    const max = msg.max;
    if (isLegacy) {
      if (!validField(id, true) || !validField(msg.name) || !validField(msg.kind)) return shut(client, CLOSE.badHello, 'bad hello');
    } else if (msg.v !== 1 || !validField(id, true) || typeof roomId !== 'string' || !ROOM_ID.test(roomId)
      || (max !== undefined && (!Number.isInteger(max) || max < 2))) {
      return shut(client, CLOSE.badHello, 'bad hello');
    }

    let room = rooms[roomId];
    const creator = room === undefined;
    if (creator) {
      room = { max: isLegacy ? Infinity : Math.min(max ?? roomMax, roomMax), size: 0, legacy: isLegacy, peers: Object.create(null) };
    }
    const peers = room.peers;
    if (peers[id]) return shut(client, CLOSE.idTaken, 'id taken');
    if (room.size >= room.max) return shut(client, CLOSE.roomFull, 'room full');
    if (creator) {
      rooms[roomId] = room;
      roomCount++;
    }
    peers[id] = client;
    room.size++;
    client.id = id;
    client.room = room;
    client.roomId = roomId;
    clearTimeout(client.helloTimer);
    guard.helloed(client.slot);
    if (grant !== null) {
      client.rate = grant.rate;
      client.burst = grant.burst;
      client.tokens = grant.burst;
    }
    if (isLegacy) {
      client.name = msg.name || id.slice(0, 6);
      client.kind = msg.kind || 'client';
      client.send(JSON.stringify({ type: 'welcome', peers: peerList(room) }));
    } else {
      client.idleTimer = setTimeout(() => shut(client, CLOSE.idle, 'idle'), idleMs);
      client.send(JSON.stringify({ type: 'welcome', peers: peerList(room), max: room.max, creator, rate: client.rate, burst: client.burst }));
    }
    log(`  hello ${id.slice(0, 6)} room=${isLegacy ? 'legacy' : roomId.slice(0, 8)}${creator ? ' (creator)' : ''} ${room.size}/${room.max}${grant !== null ? ` key=${grant.name}` : ''}`);
    broadcastPeers(room);
  }

  /** False once `client` sent offersPerMin offers to `to` within the last minute. */
  function allowOffer(client, to, now) {
    const stamps = (client.offers[to] ??= []);
    while (stamps.length && stamps[0] <= now - OFFER_WINDOW_MS) stamps.shift();
    if (stamps.length >= offersPerMin) return false;
    stamps.push(now);
    return true;
  }

  /** `target` gets `raw` unless it blocked `client`. */
  function deliver(client, target, raw) {
    const blocked = target.blocked;
    if (blocked !== null && blocked[client.id]) return;
    target.send(raw);
  }

  function route(client, msg, now) {
    const room = client.room;
    const peers = room.peers;
    const type = msg.type;
    if (type === 'signal') {
      const to = msg.to;
      const target = typeof to === 'string' ? peers[to] : undefined;
      if (target === undefined || target === client) return;
      const kind = msg.kind;
      if (!room.legacy && (!SIGNAL_KINDS[kind] || typeof msg.box !== 'string')) return;
      if (kind === 'offer' && !allowOffer(client, to, now)) return shut(client, CLOSE.rate, 'offers');
      if (room.legacy) {
        msg.from = client.id; // stamped : a peer cannot speak for another ; every other field as sent
        deliver(client, target, JSON.stringify(msg));
      } else {
        deliver(client, target, JSON.stringify({ type: 'signal', from: client.id, kind, box: msg.box }));
      }
    } else if (type === 'data') {
      const box = msg.box;
      if (room.legacy || typeof box !== 'string') return;
      const raw = JSON.stringify({ type: 'data', from: client.id, box });
      const to = msg.to;
      if (to === undefined) {
        for (const id in peers) if (peers[id] !== client) deliver(client, peers[id], raw);
      } else if (typeof to === 'string' && peers[to] !== undefined && peers[to] !== client) {
        deliver(client, peers[to], raw);
      }
      if (logData) log(`  data ${client.id.slice(0, 6)}→${to === undefined ? '*' : String(to).slice(0, 6)} ${box.slice(0, 24)}…`);
    } else if (type === 'block') {
      const id = msg.id;
      if (typeof id === 'string' && peers[id] !== undefined && peers[id] !== client) (client.blocked ??= Object.create(null))[id] = true;
    }
  }

  function leave(client) {
    const room = client.room;
    const id = client.id;
    const peers = room.peers;
    delete peers[id];
    room.size--;
    if (room.size === 0) {
      delete rooms[client.roomId];
      roomCount--;
      return;
    }
    for (const other in peers) {
      const c = peers[other];
      if (c.blocked !== null) delete c.blocked[id];
      delete c.offers[id];
    }
    broadcastPeers(room);
  }

  function onConnection(ws, req, slot) {
    const remote = `${slot.ip} :${req.socket.remotePort}`;
    const client = {
      ws,
      slot,
      id: null,
      room: null,
      roomId: null,
      name: null,
      kind: null,
      alive: true,
      rate,
      burst,
      tokens: burst,
      stamp: Date.now(),
      drops: 0,
      blocked: null,
      offers: Object.create(null),
      idleTimer: null,
      helloTimer: null,
      killTimer: null,
      send(raw) {
        if (ws.readyState === ws.OPEN) ws.send(raw);
      },
    };
    client.helloTimer = setTimeout(() => shut(client, CLOSE.key, 'no hello'), helloMs);
    ws.client = client;
    log(`+ ${remote}`);

    ws.on('pong', () => { client.alive = true; });
    ws.on('message', (raw, isBinary) => {
      if (ws.readyState !== ws.OPEN) return; // closing : what follows our close is not read (nor counted twice)
      const now = Date.now();
      client.tokens = Math.min(client.burst, client.tokens + ((now - client.stamp) * client.rate) / 1000);
      client.stamp = now;
      if (client.tokens < 1) {
        if (++client.drops >= client.burst) shut(client, CLOSE.rate, 'rate limit');
        return;
      }
      client.tokens--;
      client.drops = 0;
      if (client.idleTimer !== null) client.idleTimer.refresh();

      let msg = null;
      if (!isBinary) {
        try { msg = JSON.parse(raw.toString()); } catch { /* not JSON */ }
      }
      const valid = msg !== null && typeof msg === 'object';
      if (client.id === null) {
        if (!valid || msg.type !== 'hello') return shut(client, CLOSE.key, 'hello first');
        const isLegacy = legacy && msg.v === undefined;
        let grant = null;
        if (!isLegacy && grants.length > 0) {
          grant = grantOf(msg.key);
          if (grant === null) return shut(client, CLOSE.key, 'key');
        }
        return join(client, msg, isLegacy, grant);
      }
      if (!valid) return;
      if (msg.type === 'ping') client.send(JSON.stringify({ type: 'pong', ts: msg.ts, srv: now }));
      else route(client, msg, now);
    });
    ws.on('close', (code) => {
      clearTimeout(client.helloTimer);
      clearTimeout(client.idleTimer);
      clearTimeout(client.killTimer);
      if (client.id === null) return log(`- ${remote} (no hello, ${code})`);
      leave(client);
      log(`- ${client.id.slice(0, 6)} ${code}`);
    });
    // A 1009 (message over maxPayload) or a reset ; 'close' follows. The 1009 is a fault.
    ws.on('error', (err) => { if (err.code === 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH') guard.fault(slot.ip); });
  }

  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      const client = ws.client;
      if (!client.alive) { ws.terminate(); continue; }
      client.alive = false;
      ws.ping();
    }
  }, heartbeatMs);

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      const actual = server.address().port;
      const n = grants.length;
      const access = n > 0 ? `private, ${n} key${n > 1 ? 's' : ''} (${grants.map((g) => g.name).join(', ')})` : 'OPEN (no RELAY_KEY)';
      log(`@meitogi/signal relay listening ${tls ? 'wss' : 'ws'}://${host}:${actual} ${access}${legacy ? ' + legacy protocol' : ''}`);
      resolve({
        port: actual,
        setTls(next) {
          if (!tls) throw new Error('setTls() needs a relay started with tls');
          server.setSecureContext({ cert: next.cert, key: next.key });
        },
        stats: () => ({ rooms: roomCount, sockets: wss.clients.size, ...guard.stats() }),
        close() {
          clearInterval(heartbeat);
          guard.close();
          for (const ws of wss.clients) ws.terminate();
          wss.close();
          server.closeAllConnections();
          return new Promise((done) => server.close(() => done()));
        },
      });
    });
  });
}
