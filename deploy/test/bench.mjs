// Connectivity bench : this node side and a second peer — the signal-test-page in a browser (a phone,
// the agent browser) or a node responder (PEER=node) — meet in two rooms through a relay and run, each
// reported on both sides :
//   1. direct — room `bench` : the path the two peers got, both sides' candidates and chosen pair ;
//   2. relay  — room `bench-relay`, forced : the second peer joins it with `connectMs: 1`, so its direct
//      attempt fails at once and the relay fallback starts (only the newcomer's `connectMs` decides) ;
//      must pass on any network ; throughput measured ;
//   3. echoes — text and bytes over each path, and what the other side recorded ;
//   4. large  — one fragmented message (BIG_BYTES) echoed over each path ;
//   then the game-cadence bench (CADENCE=0 skips it), and 5. a summary, printed here and on the page.
//
// Run from packages/signal — natively on the Mac for a real direct path (the devcontainer has no LAN
// address and no IPv6) :
//   node deploy/test/bench.mjs              prints a link to open on the phone (public relay + Pages page)
// Env :
//   RELAY       wss://… (default wss://signal.du.ranchchoco.boo), or `local` : a relay in this process (RELAY_PORT)
//   LOCAL_PAGE  <port> : serve ../signal-test-page/index.html from here, its library import pointed at this tree
//   PAGE_URL    the published page (default https://meitogi.github.io/signal-test-page/)
//   PEER=node   a node responder in this process instead of a browser (loopback runs)
//   ICE=none    `iceServers: []` (loopback ; werift still asks its built-in STUN server) ; EXPECT_DIRECT=1 : no direct path fails the run
//   WAIT_MS (180000) for the second peer ; BIG_BYTES (1000000) ; FLOW_BYTES (4000000) relay throughput
//   CADENCE=0 ; RATES (30,60,120) ; LEG_MS (10000) per leg ; LATE_MS (100) ; BENCH_JSON=<file> : results as JSON
//   Each run is saved in BENCH_DIR (default deploy/test/results/, gitignored : it holds public IPs) as
//   <date>_<device>_<network>.json + .txt (the whole output), with one row appended to results.md.
//   The device comes from the peer's user agent (Brave through navigator.brave), the network from
//   navigator.connection when the browser has it, else from the peer's public IP against this machine's
//   (same-net / other-net). DEVICE=… and NET=… (e.g. NET=4g) override them ; BENCH_SAVE=0 saves nothing.
// One PASS / FAIL line per assertion, INFO for reported facts ; exit 0 = no FAIL. A direct path that does
// not come up is INFO — a symmetric NAT (Orange 4G) is a result, not a bug — unless EXPECT_DIRECT=1.
//
// Peer protocol (index.html implements the same) : a text message starting `{"bench"` is a command —
// `report` → `{"bench":"report", via, lib, ua, local, remote, pair, received, echoed}`, `summary` → lines
// to show ; any other text comes back as `echo:<text>`, any bytes come back reversed.
import { execFileSync } from 'node:child_process';
import { reverse } from 'node:dns/promises';
import { appendFileSync, createReadStream, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { register } from 'node:module';
import { dirname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

// werift's own getStats() gives the chosen pair : bench-werift.mjs records each connection, for
// node-platform.js only. The hook must be in place before the library is imported.
const HERE = dirname(fileURLToPath(import.meta.url));
register(`data:text/javascript,${encodeURIComponent(`
  const stand = ${JSON.stringify(new URL('./bench-werift.mjs', import.meta.url).href)};
  export async function resolve(specifier, context, next) {
    if (specifier === 'werift' && context.parentURL?.endsWith('/src/node-platform.js')) return { url: stand, shortCircuit: true };
    return next(specifier, context);
  }
`)}`);
const { built } = await import('./bench-werift.mjs');
const { platform } = await import('../../src/node-platform.js');
const { createClient } = await import('../../src/room.js');
const { formatLink, newSecret } = await import('../../src/node.js');
const { createRelay } = await import('../../src/relay.js');

const env = process.env;
const ROOT = join(HERE, '../..');
const PAGE_FILE = env.PAGE_FILE || join(ROOT, '../signal-test-page/index.html');
const LOCAL_PAGE = Number(env.LOCAL_PAGE || 0);
const PEER = env.PEER || 'page';
const ICE = env.ICE === 'none' ? [] : undefined;
const WAIT_MS = Number(env.WAIT_MS || 180_000);
const BIG = Number(env.BIG_BYTES || 1_000_000);
const FLOW = Number(env.FLOW_BYTES || 4_000_000);
const RATES = (env.RATES || '30,60,120').split(',').map(Number);
const LEG_MS = Number(env.LEG_MS || 10_000);
const LATE_MS = Number(env.LATE_MS || 100);
const EXPECT_DIRECT = env.EXPECT_DIRECT === '1';
// The budgets the cadence legs are sized against (src/relay-session.js, bin/signal.js, src/peer.js, src/room.js).
const SEND_RATE = 100, SEND_BURST = 200, RELAY_RATE = 120, PEER_RATE = 1000, RELAY_CHUNK = 11 * 1024, CHUNK = 16 * 1024;
const TICK_MS = 20;
const PACKET = 16;
const ROOM = 'bench';
const RELAY_ROOM = 'bench-relay';

// Everything printed is kept for the saved .txt.
const transcript = [];
const print = console.log;
console.log = (...args) => { transcript.push(args.join(' ')); print(...args); };
const say = (line) => console.log(`[bench] ${line}`);
let failed = 0;
const results = { scenarios: {}, cadence: [] };
const check = (ok, line) => { console.log(`${ok ? 'PASS' : 'FAIL'} ${line}`); if (!ok) failed++; return ok; };
const info = (line) => console.log(`INFO ${line}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const mb = (bytes, ms) => (bytes / 1e6 / (ms / 1000)).toFixed(2);

// ── candidates and chosen pair ───────────────────────────────────────────────────────────────────

/** `candidate:f 1 udp prio addr port typ host …` → `host addr:port udp` */
const cand = (line) => {
  const f = line.replace(/^a=/, '').split(' ');
  const i = f.indexOf('typ');
  return `${f[i + 1]} ${f[4]}:${f[5]} ${f[2].toLowerCase()}`;
};

/** The chosen pair from W3C-shaped stats (a browser's, or werift's). */
function selectedPair(stats) {
  const byId = {};
  for (const s of stats) byId[s.id] = s;
  let pair = null;
  for (const s of stats) if (s.type === 'transport' && s.selectedCandidatePairId && byId[s.selectedCandidatePairId]) pair = byId[s.selectedCandidatePairId];
  if (!pair) pair = stats.find((s) => s.type === 'candidate-pair' && s.nominated && s.state === 'succeeded') ?? stats.find((s) => s.type === 'candidate-pair' && s.nominated) ?? null;
  if (!pair) return null;
  const side = (c) => (c ? `${c.candidateType} ${c.address ?? c.ip}:${c.port}` : '?');
  const local = byId[pair.localCandidateId];
  return `${side(local)} ↔ ${side(byId[pair.remoteCandidateId])} ${local?.protocol ?? ''}`.trim();
}

/** A joinRoom whose connections are recorded in `pcs` : candidates sent and received, werift's connection. */
function recorded() {
  const pcs = [];
  class Rec extends platform.RTCPeerConnection {
    constructor(config) {
      super(config);
      this._rec = { inner: built.at(-1), local: [], remote: [] };
      pcs.push(this._rec);
      this.addEventListener('icecandidate', ({ candidate }) => { if (candidate?.candidate) this._rec.local.push(candidate.candidate); });
    }
    addIceCandidate(c) {
      if (c?.candidate) this._rec.remote.push(c.candidate);
      return super.addIceCandidate(c);
    }
  }
  return { pcs, joinRoom: createClient({ ...platform, RTCPeerConnection: Rec }).joinRoom };
}

async function facts(pcs) {
  const rec = pcs.at(-1);
  if (!rec) return { local: [], remote: [], pair: null };
  let pair = null;
  try { pair = selectedPair([...(await rec.inner.getStats()).values()]); } catch { /* closed */ }
  const sdp = rec.inner.localDescription?.sdp.match(/a=candidate:[^\r\n]+/g) ?? [];
  const unique = (v, i, list) => list.indexOf(v) === i;
  return { local: [...rec.local, ...sdp].map(cand).filter(unique), remote: rec.remote.map(cand).filter(unique), pair };
}

// ── messages ─────────────────────────────────────────────────────────────────────────────────────

/** A peer's messages as a queue ; `bytes` (when set) takes binary messages instead. */
function inbox(peer) {
  const queue = [];
  const waiters = [];
  const box = { bytes: null };
  peer.on('message', (data) => {
    if (typeof data !== 'string' && box.bytes) return box.bytes(data);
    const i = waiters.findIndex((w) => w.match(data));
    if (i >= 0) return waiters.splice(i, 1)[0].resolve(data);
    queue.push(data);
  });
  box.next = (match, ms = 15_000, what = 'a message') => {
    const i = queue.findIndex(match);
    if (i >= 0) return Promise.resolve(queue.splice(i, 1)[0]);
    return new Promise((resolve, reject) => {
      const w = { match, resolve: (d) => { clearTimeout(t); resolve(d); } };
      const t = setTimeout(() => { waiters.splice(waiters.indexOf(w), 1); reject(new Error(`no ${what} within ${ms} ms`)); }, ms);
      waiters.push(w);
    });
  };
  return box;
}

const isBench = (kind) => (d) => typeof d === 'string' && d.startsWith('{"bench"') && JSON.parse(d).bench === kind;
const reversed = (bytes) => bytes.slice().reverse();
const same = (a, b) => a.length === b.length && a.every((v, i) => v === b[i]);
const random = (n) => { const b = new Uint8Array(n); for (let i = 0; i < n; i += 65_536) crypto.getRandomValues(b.subarray(i, i + 65_536)); return b; };

async function askReport(link) {
  await link.peer.send('{"bench":"report"}');
  return JSON.parse(await link.box.next(isBench('report'), 15_000, 'report'));
}

// ── library version on both sides ────────────────────────────────────────────────────────────────

const git = (...args) => execFileSync('git', ['-C', ROOT, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();

function sameLibrary(lib) {
  if (lib === '/src/index.js') return check(true, `library : the peer imports this tree's src/ (served from here)`);
  if (lib === 'node:local') return check(true, 'library : the peer is this process (same tree)');
  const sha = /\/gh\/meitogi\/signal@([0-9a-f]{40})\/src\//.exec(lib ?? '')?.[1];
  if (!sha) return check(false, `library : the peer imports ${lib ?? 'nothing reported'} — not pinned to a full commit`);
  try {
    const head = git('rev-parse', 'HEAD');
    const dirty = git('status', '--porcelain', '--', 'src') !== '';
    const theirs = git('rev-parse', `${sha}:src`);
    const ours = git('rev-parse', 'HEAD:src');
    info(`library : node side ${head.slice(0, 7)} (src/ tree ${ours.slice(0, 7)}${dirty ? ', modified' : ''}), peer @${sha.slice(0, 7)} (src/ tree ${theirs.slice(0, 7)})`);
    return check(theirs === ours && !dirty, `library : same src/ on both sides${dirty ? ' — FAIL : src/ modified here' : ''}`);
  } catch {
    info(`library : peer @${sha.slice(0, 7)} ; cannot compare here (no git, or commit ${sha.slice(0, 7)} unknown locally)`);
    return true;
  }
}

// ── who the peer is, on which network ────────────────────────────────────────────────────────────

/** `ios-18.7-safari-18.7`, `macos-brave`, `macos-firefox-131`… from the user agent the peer reported. */
function clientName(report) {
  const ua = report.ua ?? '';
  if (ua.startsWith('node ')) return { os: process.platform, browser: 'node', label: env.DEVICE || 'node' };
  let m;
  let os = 'unknown';
  if ((m = /(iPhone|iPad)[^)]*OS (\d+)[_.](\d+)/.exec(ua))) os = `${m[1] === 'iPad' ? 'ipados' : 'ios'}-${m[2]}.${m[3]}`;
  else if ((m = /Android (\d+(?:\.\d+)?)/.exec(ua))) os = `android-${m[1]}`;
  else if (/Macintosh|Mac OS X/.test(ua)) os = 'macos';
  else if (/Windows/.test(ua)) os = 'windows';
  else if (/Linux/.test(ua)) os = 'linux';
  let browser = 'unknown';
  if (report.client?.brave) browser = 'brave'; // Brave's user agent is Chrome's
  else if ((m = /(?:Firefox|FxiOS)\/(\d+)/.exec(ua))) browser = `firefox-${m[1]}`;
  else if ((m = /(?:EdgiOS|EdgA|Edg)\/(\d+)/.exec(ua))) browser = `edge-${m[1]}`;
  else if ((m = /(?:CriOS|Chrome)\/(\d+)/.exec(ua))) browser = `chrome-${m[1]}`;
  else if ((m = /Version\/(\d+(?:\.\d+)?).*Safari/.exec(ua))) browser = `safari-${m[1]}`;
  return { os, browser, label: env.DEVICE || `${os}-${browser}` };
}

/** Wi-Fi or cellular when the browser says so (navigator.connection, Chromium on Android), else a guess from public IPs. */
async function networkGuess(report, mine) {
  const publicIps = (list) => list.filter((c) => c.startsWith('srflx ')).map((c) => c.split(' ')[1].replace(/:\d+$/, '')).filter((v, i, a) => a.indexOf(v) === i);
  const peerPublic = publicIps(report.local ?? []);
  const nodePublic = publicIps(mine.local);
  const reverseDns = [];
  for (const ip of peerPublic) reverseDns.push(...(await Promise.race([reverse(ip).catch(() => []), sleep(2000).then(() => [])])));
  const c = report.client?.connection ?? null;
  let guess = 'unknown';
  let source = 'the peer reported no public (srflx) candidate';
  if (c?.type) { guess = c.type; source = 'navigator.connection.type'; }
  else if (peerPublic.some((ip) => nodePublic.includes(ip))) { guess = 'same-net'; source = 'same public IP as this machine (same Wi-Fi / LAN)'; }
  else if (peerPublic.length) { guess = 'other-net'; source = 'public IP differs from this machine (cellular, or another network)'; }
  return { label: env.NET || guess, guess, source, peerPublic, nodePublic, reverseDns, connection: c };
}

function save() {
  const dir = env.BENCH_DIR || join(HERE, 'results');
  mkdirSync(dir, { recursive: true });
  const slug = (text) => String(text).toLowerCase().replace(/[^a-z0-9.]+/g, '-').replace(/^-|-$/g, '');
  const stamp = new Date().toISOString().slice(0, 16).replace('T', '_').replace(':', '');
  const base = join(dir, `${stamp}_${slug(results.client?.label ?? 'no-peer')}_${slug(results.network?.label ?? 'unknown')}`);
  writeFileSync(`${base}.json`, JSON.stringify(results, null, 2));
  writeFileSync(`${base}.txt`, `${transcript.join('\n')}\n`);
  const index = join(dir, 'results.md');
  if (!existsSync(index)) {
    appendFileSync(index, '| date (UTC) | device | network | direct | pair (node side) | relay MB/s | relay 60 Hz p50 / p95 | direct 60 Hz p50 / p95 | result |\n|---|---|---|---|---|---|---|---|---|\n');
  }
  const leg = (path) => results.cadence.find((l) => l.path === path && l.rate === 60 && !l.batch);
  const rtt = (l) => (l ? `${l.p50.toFixed(1)} / ${l.p95.toFixed(1)} ms` : '—');
  const s = results.scenarios;
  const cells = [
    stamp.replace('_', ' '), results.client?.label ?? '—', results.network ? `${results.network.label} (${results.network.source})` : '—',
    s.direct?.via ?? '—', s.direct?.node.pair ?? '—', s.relay?.mbPerS ?? '—', rtt(leg('relay')), rtt(leg('direct')),
    results.error ? `FAIL : ${results.error}` : results.failed ? `FAIL × ${results.failed}` : 'PASS',
  ];
  appendFileSync(index, `| ${cells.join(' | ')} |\n`);
  print(`[bench] saved ${base}.json and .txt ; row added to ${index}`);
}

// ── the responder (PEER=node) : the page's bench mode, in node ───────────────────────────────────

async function responder(parts) {
  const serve = (peer, pcs, state) => peer.on('message', async (data) => {
    if (typeof data === 'string' && data.startsWith('{"bench"')) {
      const msg = JSON.parse(data);
      if (msg.bench === 'report') await peer.send(JSON.stringify({ bench: 'report', via: peer.via, lib: 'node:local', ua: `node ${process.version}`, received: state.received, echoed: state.echoed, ...(await facts(pcs)) }));
      return;
    }
    if (typeof data === 'string') {
      state.received.push(data);
      return peer.send(`echo:${data}`).catch(() => {});
    }
    if (data.length <= 64) state.received.push(`bytes:${data.join(',')}`);
    state.echoed++;
    peer.send(reversed(data)).catch(() => {});
  });
  const enter = async (room, options) => {
    const { pcs, joinRoom } = recorded();
    const r = joinRoom({ ...parts, room, iceServers: ICE, ...options });
    const state = { received: [], echoed: 0 };
    r.on('peer', (peer) => serve(peer, pcs, state));
    await new Promise((resolve) => { r.on('peer', resolve); setTimeout(resolve, 40_000); });
    return r;
  };
  return [await enter(ROOM, { connectMs: 15_000 }), await enter(RELAY_ROOM, { connectMs: 1 })];
}

// ── game cadence ─────────────────────────────────────────────────────────────────────────────────

const pct = (sorted, p) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] : NaN);
const median = (list) => pct([...list].sort((a, b) => a - b), 50);
const ms = (v) => (Number.isFinite(v) ? `${v.toFixed(1)} ms` : '—');

/** What the leg sends per second from each side, and what that means against the budgets. */
function sizing(path, rate, batch) {
  const sends = batch ? Math.min(rate, 1000 / TICK_MS) : rate;
  if (path === 'direct') return { sends, line: `${sends}/s per side, direct : vs ${PEER_RATE} frames/s receive cap — fits`, queueS: 0 };
  // The bucket starts full (SEND_BURST) ; above SEND_RATE it drains by (sends - SEND_RATE)/s, then the queue grows.
  const backlog = Math.max(0, (sends - SEND_RATE) * (LEG_MS / 1000) - SEND_BURST);
  const queueS = backlog / SEND_RATE;
  const verdict = sends <= SEND_RATE ? 'fits' : `over the bucket by ${sends - SEND_RATE}/s : burst lasts ${(SEND_BURST / (sends - SEND_RATE)).toFixed(1)} s, predicted queue at the end ${queueS.toFixed(1)} s (waits, never dropped)`;
  return { sends, queueS, line: `${sends}/s per side, relay : vs bucket ${SEND_RATE}/s burst ${SEND_BURST} per room, relay ${RELAY_RATE}/s per socket — ${verdict}` };
}

let legId = 0;
async function cadenceLeg(link, path, rate, batch) {
  const id = ++legId;
  const total = Math.round((rate * LEG_MS) / 1000);
  const sentAt = new Array(total);
  const rtt = new Array(total);
  let echoed = 0;
  let sends = 0;
  const size = sizing(path, rate, batch);
  say(`leg ${path} ${rate} Hz ${batch ? `batched ${TICK_MS} ms` : 'unbatched'} — ${size.line}`);
  link.box.bytes = (data) => {
    const now = performance.now();
    const back = reversed(data);
    const view = new DataView(back.buffer, back.byteOffset, back.byteLength);
    for (let o = 0; o + PACKET <= back.length; o += PACKET) {
      if (view.getUint32(o) !== id) continue;
      const seq = view.getUint32(o + 4);
      if (seq < total && rtt[seq] === undefined) { rtt[seq] = now - sentAt[seq]; echoed++; }
    }
  };
  const send = (bytes) => { sends++; link.peer.send(bytes).catch(() => {}); };
  const packet = (seq) => {
    const b = new Uint8Array(PACKET);
    const view = new DataView(b.buffer);
    view.setUint32(0, id);
    view.setUint32(4, seq);
    return b;
  };
  let pending = [];
  const flush = () => {
    if (!pending.length) return;
    const out = new Uint8Array(pending.length * PACKET);
    pending.forEach((p, i) => out.set(p, i * PACKET));
    pending = [];
    send(out);
  };
  const start = performance.now();
  let seq = 0;
  await new Promise((resolve) => {
    const tick = setInterval(() => {
      const due = Math.min(total, Math.floor(((performance.now() - start) * rate) / 1000) + 1);
      for (; seq < due; seq++) {
        const p = packet(seq);
        sentAt[seq] = performance.now();
        if (batch) pending.push(p);
        else send(p);
      }
      if (seq >= total) { clearInterval(tick); clearInterval(batcher); flush(); resolve(); }
    }, 2);
    const batcher = batch ? setInterval(flush, TICK_MS) : null;
  });
  const sendMs = performance.now() - start;
  const drainUntil = performance.now() + size.queueS * 2000 + 5000;
  while (echoed < total && performance.now() < drainUntil) await sleep(50);
  link.box.bytes = null;

  const got = rtt.filter((v) => v !== undefined);
  const sorted = [...got].sort((a, b) => a - b);
  let jitter = 0;
  let pairs = 0;
  for (let i = 1; i < total; i++) if (rtt[i] !== undefined && rtt[i - 1] !== undefined) { jitter += Math.abs(rtt[i] - rtt[i - 1]); pairs++; }
  const late = got.filter((v) => v > LATE_MS).length;
  const firstS = rtt.slice(0, rate).filter((v) => v !== undefined);
  const lastS = rtt.slice(-rate).filter((v) => v !== undefined);
  const leg = {
    path, rate, batch, packets: total, sends, sendsPerS: +(sends / (sendMs / 1000)).toFixed(1), expectedSendsPerS: size.sends,
    predictedQueueS: +size.queueS.toFixed(2), echoed, missing: total - echoed, late, lost: total - echoed + late,
    lossPct: +(((total - echoed + late) / total) * 100).toFixed(1),
    p50: pct(sorted, 50), p95: pct(sorted, 95), p99: pct(sorted, 99), max: sorted.at(-1) ?? NaN,
    jitter: pairs ? jitter / pairs : NaN, firstSecondMedian: median(firstS), lastSecondMedian: median(lastS),
  };
  results.cadence.push(leg);
  info(`  sent ${total} packets in ${sends} sends (${leg.sendsPerS}/s) ; echoed ${echoed} ; lost ${leg.lost} (${leg.lossPct} %) = ${leg.missing} never back + ${late} over ${LATE_MS} ms`);
  info(`  RTT p50 ${ms(leg.p50)} p95 ${ms(leg.p95)} p99 ${ms(leg.p99)} max ${ms(leg.max)} ; jitter ${ms(leg.jitter)} ; median first second ${ms(leg.firstSecondMedian)} → last second ${ms(leg.lastSecondMedian)}`);
  return leg;
}

// ── run ──────────────────────────────────────────────────────────────────────────────────────────

let relay = null;
let relayUrl = env.RELAY || 'wss://signal.du.ranchchoco.boo';
let pageRelay = relayUrl;
if (relayUrl === 'local') {
  relay = await createRelay({ port: Number(env.RELAY_PORT || 0), host: '0.0.0.0', log: () => {} });
  relayUrl = `ws://127.0.0.1:${relay.port}`;
  pageRelay = `ws://localhost:${relay.port}`;
}

let server = null;
if (LOCAL_PAGE) {
  const html = readFileSync(PAGE_FILE, 'utf8').replace(/https:\/\/cdn\.jsdelivr\.net\/gh\/meitogi\/signal@[\w.]+\/src\/index\.js/, '/src/index.js');
  server = createServer((req, res) => {
    const path = normalize(decodeURIComponent(new URL(req.url, 'http://x').pathname));
    if (path === '/') { res.writeHead(200, { 'content-type': 'text/html' }); return res.end(html); }
    const file = join(ROOT, path);
    if (!path.startsWith('/src/') || !existsSync(file)) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'content-type': 'text/javascript' });
    createReadStream(file).pipe(res);
  });
  await new Promise((r) => server.listen(LOCAL_PAGE, '0.0.0.0', r));
}
const PAGE = LOCAL_PAGE ? `http://localhost:${LOCAL_PAGE}/` : env.PAGE_URL || 'https://meitogi.github.io/signal-test-page/';

const secret = newSecret();
const rooms = {};
const links = {};
let others = [];
let code = 1;
try {
  for (const [key, name] of [['direct', ROOM], ['relay', RELAY_ROOM]]) {
    const { pcs, joinRoom } = recorded();
    const room = joinRoom({ relay: relayUrl, room: name, secret, iceServers: ICE });
    room.on('error', (e) => info(`room ${name} : ${e.code} ${e.message}`));
    const first = new Promise((resolve) => room.on('peer', resolve));
    await room.ready;
    rooms[key] = { room, pcs, first };
  }
  const link = `${formatLink(PAGE, { room: ROOM, secret, relay: pageRelay })}&mode=bench`;
  say(`relay ${relayUrl} ; node ${process.version} ${process.platform} ; iceServers ${ICE ? '[] (werift keeps its own STUN default)' : 'library default (STUN)'}`);
  if (PEER === 'node') {
    others = await responder({ relay: relayUrl, secret });
  } else {
    say(`open this link in the browser (the phone : 4G with Wi-Fi off, or the same Wi-Fi), within ${WAIT_MS / 1000} s :`);
    console.log(`\n    ${link}\n`);
  }
  const arrive = (key, ms) => Promise.race([rooms[key].first, sleep(ms).then(() => { throw new Error(`no peer in room ${key} within ${ms / 1000} s`); })]);
  for (const key of ['direct', 'relay']) {
    const peer = await arrive(key, key === 'direct' ? WAIT_MS : 60_000);
    links[key] = { peer, box: inbox(peer) };
    peer.on('close', (reason) => info(`peer in room ${key} closed : ${reason}`));
  }

  // 1. direct
  say('— 1. direct');
  const d = links.direct;
  const mine = await facts(rooms.direct.pcs);
  const theirs = await askReport(d);
  results.scenarios.direct = { via: d.peer.via, peerVia: theirs.via, node: mine, peer: { local: theirs.local, remote: theirs.remote, pair: theirs.pair, ua: theirs.ua } };
  info(`peer : ${theirs.ua}`);
  results.client = clientName(theirs);
  results.network = await networkGuess(theirs, mine);
  info(`device : ${results.client.label}${env.DEVICE ? ' (DEVICE)' : ''} ; network : ${results.network.label}${env.NET ? ' (NET)' : ''} — ${results.network.source}`);
  if (results.network.connection) info(`  navigator.connection : ${JSON.stringify(results.network.connection)} (effectiveType is a speed class, not the radio : « 4g » on a fast Wi-Fi too)`);
  if (results.network.peerPublic.length) info(`  peer public ${results.network.peerPublic.join(', ')}${results.network.reverseDns.length ? ` (${results.network.reverseDns.join(', ')})` : ''} ; this machine ${results.network.nodePublic.join(', ') || 'none'}`);
  sameLibrary(theirs.lib);
  for (const c of mine.local) info(`node local  ${c}`);
  for (const c of mine.remote) info(`node remote ${c}`);
  info(`node pair   ${mine.pair ?? 'none (no direct path)'}`);
  for (const c of theirs.local) info(`peer local  ${c}`);
  for (const c of theirs.remote) info(`peer remote ${c}`);
  info(`peer pair   ${theirs.pair ?? 'none (no direct path)'}`);
  check(theirs.via === d.peer.via, `both sides agree on the path : node via ${d.peer.via}, peer via ${theirs.via}`);
  if (d.peer.via === 'direct') check(true, 'direct path established');
  else if (EXPECT_DIRECT) check(false, 'direct path established (EXPECT_DIRECT=1) — the peers fell back to the relay');
  else info('direct path not established : the room fell back to the relay (a symmetric NAT, e.g. 4G, does this) — reported, not failed');

  // 2. relay, forced
  say('— 2. relay fallback, forced (the peer joined with connectMs: 1)');
  const r = links.relay;
  const rTheirs = await askReport(r);
  check(r.peer.via === 'relay' && rTheirs.via === 'relay', `relay path : node via ${r.peer.via}, peer via ${rTheirs.via}`);
  const part = 250_000;
  const flow = random(part);
  const n = Math.max(1, Math.round(FLOW / part));
  const frames = n * Math.ceil(part / RELAY_CHUNK);
  const t0 = performance.now();
  let back = 0;
  r.box.bytes = (data) => { if (data.length === part) back++; };
  for (let i = 0; i < n; i++) r.peer.send(flow).catch(() => {});
  while (back < n && performance.now() - t0 < 120_000) await sleep(20);
  r.box.bytes = null;
  const flowMs = performance.now() - t0;
  const expected = Math.max(0, frames - SEND_BURST) / SEND_RATE;
  results.scenarios.relay = { via: r.peer.via, peerVia: rTheirs.via, flowBytes: n * part, frames, ms: Math.round(flowMs), mbPerS: +mb(n * part, flowMs), expectedS: expected };
  check(back === n, `relay throughput : ${n} × ${part} B sent and echoed (${frames} frames of ≤ ${RELAY_CHUNK} B each way) in ${(flowMs / 1000).toFixed(1)} s ≈ ${mb(n * part, flowMs)} MB/s each way`);
  info(`  budget : ${SEND_RATE} frames/s × ${RELAY_CHUNK} B ≈ ${mb(SEND_RATE * RELAY_CHUNK, 1000)} MB/s per side after a ${SEND_BURST}-frame burst → expected ≥ ${expected.toFixed(1)} s`);
  await sleep(2500); // the bucket refills before the next scenario

  // 3. echoes, 4. large — over each path
  results.scenarios.echo = {};
  results.scenarios.large = {};
  for (const key of ['direct', 'relay']) {
    const l = links[key];
    const via = l.peer.via;
    say(`— 3. echoes, room ${key} (via ${via})`);
    const text = `bonjour ${key} — é ✓`;
    await l.peer.send(text);
    await l.peer.send(new Uint8Array([1, 2, 3, 4]));
    const t = await l.box.next((m) => typeof m === 'string' && m.startsWith('echo:'), 15_000, 'text echo').catch((e) => e.message);
    const b = await l.box.next((m) => typeof m !== 'string', 15_000, 'bytes echo').catch(() => null);
    const report = await askReport(l);
    const okText = check(t === `echo:${text}`, `text echo via ${via} : ${JSON.stringify(t)}`);
    const okBytes = check(b !== null && same(b, [4, 3, 2, 1]), `bytes echo via ${via} : ${b ? b.join(',') : 'none'}`);
    const okPeer = check(report.received.includes(text) && report.received.includes('bytes:1,2,3,4'), `peer side recorded both : ${JSON.stringify(report.received.slice(-2))}`);
    results.scenarios.echo[key] = { via, ok: okText && okBytes && okPeer };

    say(`— 4. one large message, room ${key} (via ${via})`);
    const big = random(BIG);
    const tb = performance.now();
    await l.peer.send(big);
    const echo = await l.box.next((m) => typeof m !== 'string' && m.length === BIG, 60_000, 'large echo').catch(() => null);
    const bigMs = performance.now() - tb;
    const fragments = Math.ceil(BIG / (via === 'relay' ? RELAY_CHUNK : CHUNK));
    const ok = check(echo !== null && same(echo, reversed(big)), `${BIG} B in ${fragments} frames each way via ${via}, echoed intact in ${(bigMs / 1000).toFixed(2)} s (≈ ${mb(2 * BIG, bigMs)} MB/s round trip${via === 'relay' && fragments <= SEND_BURST ? `, within the ${SEND_BURST}-frame burst` : ''})`);
    results.scenarios.large[key] = { via, bytes: BIG, fragments, ms: Math.round(bigMs), ok };
    if (via === 'relay') await sleep(2500);
  }

  // game cadence
  if (env.CADENCE !== '0') {
    say(`— game cadence : node sends fixed-rate ${PACKET}-byte packets, the peer echoes each message ; latency = RTT from the echo (no clock offset needed)`);
    say(`  loss = never echoed, or echoed after ${LATE_MS} ms (the channel is reliable and ordered : a lost packet shows up late) ; jitter = mean |RTT(n) - RTT(n-1)|`);
    say(`  batched = the packets of each ${TICK_MS} ms tick in one message (its RTT includes the wait for the tick)`);
    const paths = links.direct.peer.via === 'direct' ? ['direct', 'relay'] : ['relay'];
    if (paths.length === 1) info('no direct path : direct legs skipped');
    for (const path of paths) {
      for (const rate of RATES) {
        for (const batch of [false, true]) {
          await cadenceLeg(links[path], path, rate, batch);
          await sleep(2500); // bucket full again for the next leg
        }
      }
    }
    for (const leg of results.cadence) check(leg.echoed > 0 && !links[leg.path].peer.closed, `cadence ${leg.path} ${leg.rate} Hz ${leg.batch ? 'batched' : 'unbatched'} : echoes came back, peer still open`);
  }

  // 5. summary
  const s = results.scenarios;
  const lines = [
    `@meitogi/signal bench — ${new Date().toISOString()}`,
    `1. direct : node via ${s.direct.via}, peer via ${s.direct.peerVia} ; pair node ${s.direct.node.pair ?? '—'} ; pair peer ${s.direct.peer.pair ?? '—'}`,
    `2. relay forced : via ${s.relay.via} / ${s.relay.peerVia} ; ${s.relay.flowBytes} B each way in ${(s.relay.ms / 1000).toFixed(1)} s ≈ ${s.relay.mbPerS} MB/s`,
    `3. echoes : ${Object.entries(s.echo).map(([k, v]) => `${k} (via ${v.via}) ${v.ok ? 'ok' : 'FAIL'}`).join(' ; ')}`,
    `4. large ${BIG} B : ${Object.entries(s.large).map(([k, v]) => `${k} (via ${v.via}) ${v.ok ? `${(v.ms / 1000).toFixed(2)} s` : 'FAIL'}`).join(' ; ')}`,
    ...results.cadence.map((l) => `cadence ${l.path} ${l.rate} Hz ${l.batch ? 'batched' : 'unbatched'} : ${l.sendsPerS} sends/s, RTT p50 ${ms(l.p50)} p95 ${ms(l.p95)}, jitter ${ms(l.jitter)}, lost ${l.lossPct} %`),
    failed ? `FAIL — ${failed} assertion(s)` : 'PASS',
  ];
  console.log(`\n${lines.join('\n')}\n`);
  for (const key of ['direct', 'relay']) if (!links[key].peer.closed) await links[key].peer.send(JSON.stringify({ bench: 'summary', lines })).catch(() => {});
  results.failed = failed;
  results.summary = lines;
  code = failed ? 1 : 0;
} catch (e) {
  console.log(`FAIL ${e.message}`);
  results.error = e.message;
} finally {
  if (env.BENCH_JSON) writeFileSync(env.BENCH_JSON, JSON.stringify(results, null, 2));
  if (env.BENCH_SAVE !== '0') save();
  await sleep(500); // the summary reaches the page
  for (const key in rooms) rooms[key].room.leave();
  for (const o of others) o.leave();
  server?.close();
  await relay?.close();
  process.exit(code);
}
