// Probes a relay through a proxy : wss connect, key check, sealed-box forwarding, /healthz, the hello
// delay, and the per-IP cap on open sockets. Runs inside the signal-relay image (it has `ws`) on the
// proxy's network :
//   node probe.mjs <wss-url>   with RELAY_KEY and EXPECT_OPEN (sockets one IP may hold) in the environment.
// Prints `probe-ip <addr>` so run.sh can check the relay logged the probe's IP, not the proxy's.
import { randomBytes } from 'node:crypto';
import { networkInterfaces } from 'node:os';
import WebSocket from 'ws';

const url = process.argv[2];
const key = process.env.RELAY_KEY;
const expectOpen = Number(process.env.EXPECT_OPEN);
const tls = { rejectUnauthorized: false }; // test proxies use self-signed / internal-CA certs
let failed = 0;

function check(name, ok, detail = '') {
  console.log(`${ok ? 'PASS' : 'FAIL'} ${name}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed++;
}

function open(hello) {
  const ws = new WebSocket(url, tls);
  const messages = [];
  const state = { ws, messages, closeCode: null, status: 0, openedAt: 0, closedAt: 0 };
  state.closed = new Promise((resolve) => ws.on('close', (code) => {
    state.closeCode = code;
    state.closedAt = Date.now();
    resolve(code);
  }));
  state.opened = new Promise((resolve) => {
    ws.on('open', () => {
      state.status = 101;
      state.openedAt = Date.now();
      if (hello) ws.send(JSON.stringify(hello));
      resolve(101);
    });
    ws.on('unexpected-response', (req, res) => { state.status = res.statusCode; res.resume(); resolve(res.statusCode); });
    ws.on('error', () => resolve(state.status));
  });
  ws.on('message', (raw) => messages.push(JSON.parse(raw.toString())));
  return state;
}

const waitFor = async (pred, ms) => {
  for (const end = Date.now() + ms; Date.now() < end;) {
    if (pred()) return true;
    await new Promise((r) => setTimeout(r, 20));
  }
  return pred();
};

for (const list of Object.values(networkInterfaces())) {
  for (const a of list) if (a.family === 'IPv4' && !a.internal) console.log(`probe-ip ${a.address}`);
}

// The proxy picks the relay up a moment after it is healthy : wait for the route first.
const healthUrl = url.replace(/^wss:/, 'https:') + '/healthz';
let routed = false;
for (const end = Date.now() + 30_000; !routed && Date.now() < end;) {
  routed = (await fetch(healthUrl).catch(() => ({ status: 0 }))).status === 200;
  if (!routed) await new Promise((r) => setTimeout(r, 500));
}
check('GET /healthz through the proxy', routed);
if (!routed) process.exit(1);

const room = randomBytes(32).toString('base64url');
const a = open({ type: 'hello', v: 1, id: 'pa', room, key });
const b = open({ type: 'hello', v: 1, id: 'pb', room, key });
await Promise.all([a.opened, b.opened]);
check('wss connect through the proxy, right key → welcome',
  await waitFor(() => a.messages.some((m) => m.type === 'welcome') && b.messages.some((m) => m.type === 'welcome'), 3000));
a.ws.send(JSON.stringify({ type: 'signal', to: 'pb', kind: 'offer', box: 'cHJvYmU=' }));
check('signal forwarded through the proxy, from stamped',
  await waitFor(() => b.messages.some((m) => m.type === 'signal' && m.from === 'pa' && m.box === 'cHJvYmU='), 3000));
a.ws.close(); b.ws.close();
await Promise.all([a.closed, b.closed]);

const wrong = open({ type: 'hello', v: 1, id: 'pw', room, key: key + '-wrong' });
await wrong.opened;
await wrong.closed;
check('wrong key → closed 4401, no welcome', wrong.closeCode === 4401 && !wrong.messages.length, `code ${wrong.closeCode}`);

const silent = open(null);
await silent.opened;
await silent.closed;
const after = silent.closedAt - silent.openedAt;
check('no hello → closed 4401 after ~5 s', silent.closeCode === 4401 && after >= 4500 && after < 7000, `code ${silent.closeCode} after ${after} ms`);

// Last : silent sockets from this one IP. Past the cap (the proxy's, or the relay's per-IP one,
// which only works when the relay sees this IP through X-Forwarded-For) they get a 429.
const many = [];
for (let i = 0; i < expectOpen + 4; i++) {
  many.push(open(null));
  await many.at(-1).opened;
}
const statuses = many.map((s) => s.status);
const opened = statuses.filter((s) => s === 101).length;
check(`one IP holds ${expectOpen} open sockets, the rest get 429`, opened === expectOpen && statuses.slice(opened).every((s) => s === 429),
  statuses.join(','));
for (const s of many) s.ws.terminate();

process.exit(failed ? 1 : 0);
