// Browser ↔ Node smoke : the real browser entry in the agent browser (host Chromium over CDP) talks
// to a Node peer in this container through a relay on :8788. Not part of `npm test` (needs the
// agent browser) : `node test/browser/smoke.mjs`. Exits 0 when the exchange is proven both ways.
import { spawnSync } from 'node:child_process';
import { createReadStream, existsSync } from 'node:fs';
import { createServer } from 'node:http';
import { dirname, extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRelay } from '../../src/relay.js';
import { formatLink, joinRoom, newSecret } from '../../src/node.js';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '../..');
const PAGE_PORT = 8091;
const RELAY_PORT = 8788;
const CDP = '/opt/devcontainer/base/skills/visual-loop/scripts/cdp.mjs';
const TYPES = { '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript' };
const log = (line) => console.log(`[smoke] ${line}`);

const sh = (cmd) => spawnSync('bash', ['-lc', cmd], { encoding: 'utf8' });
const env = sh('agent-browser env');
if (env.status !== 0) { log(`agent-browser env failed (${env.status}) : ${env.stderr || env.stdout}`); process.exit(3); }
const cdpEnv = Object.fromEntries([...env.stdout.matchAll(/\b(CDP_\w+)="?([^\s"]+)"?/g)].map((m) => [m[1], m[2]]));
log(`CDP ${cdpEnv.CDP_HOST}:${cdpEnv.CDP_PORT}`);

const server = createServer((req, res) => {
  const path = normalize(decodeURIComponent(new URL(req.url, 'http://x').pathname));
  const ok = path.startsWith('/src/') || path.startsWith('/test/browser/');
  const file = join(ROOT, path);
  if (!ok || !existsSync(file)) { res.writeHead(404); return res.end(); }
  res.writeHead(200, { 'content-type': TYPES[extname(file)] ?? 'application/octet-stream' });
  createReadStream(file).pipe(res);
});
await new Promise((r) => server.listen(PAGE_PORT, '0.0.0.0', r));
const relay = await createRelay({ port: RELAY_PORT, host: '0.0.0.0', log: (l) => log(`relay: ${l}`) });

const secret = newSecret();
const link = formatLink(`http://localhost:${PAGE_PORT}/test/browser/index.html`, { room: 'smoke', secret, relay: `ws://localhost:${RELAY_PORT}` });
const node = joinRoom({ relay: `ws://127.0.0.1:${RELAY_PORT}`, room: 'smoke', secret, iceServers: [], connectMs: 8000 });
node.on('error', (e) => log(`node error ${e.code} ${e.message}`));
await node.ready;
log(`node in the room as ${node.id} (creator ${node.isCreator})`);

const evalJs = (js) => {
  const r = spawnSync('node', [CDP, 'eval', '--json', '--', js], { encoding: 'utf8', env: { ...process.env, ...cdpEnv } });
  if (r.status !== 0) throw new Error(`cdp eval failed : ${r.stderr || r.stdout}`);
  const out = r.stdout.trim();
  try { return JSON.parse(out); } catch { return out; }
};

let code = 1;
try {
  const up = sh(`agent-browser up '${link}'`);
  if (up.status !== 0) throw new Error(`agent-browser up failed (${up.status}) : ${up.stderr || up.stdout}`);
  log(up.stdout.trim().split('\n').pop());
  const peer = await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('no peer within 25 s')), 25_000);
    node.on('peer', (p) => { clearTimeout(t); resolve(p); });
  });
  log(`node sees browser peer ${peer.id} via ${peer.via}`);
  const echoes = [];
  peer.on('message', (m) => echoes.push(m));
  await peer.send('bonjour navigateur');
  await peer.send(new Uint8Array([1, 2, 3]));
  const until = Date.now() + 10_000;
  while (echoes.length < 2 && Date.now() < until) await new Promise((r) => setTimeout(r, 100));
  if (echoes.length < 2) throw new Error(`echoes missing : ${JSON.stringify(echoes)}`);
  if (echoes[0] !== 'echo:bonjour navigateur') throw new Error(`bad text echo ${echoes[0]}`);
  if (!(echoes[1] instanceof Uint8Array) || [...echoes[1]].join() !== '3,2,1') throw new Error(`bad bytes echo ${echoes[1]}`);
  const state = evalJs('JSON.stringify(window.state)');
  const s = JSON.parse(typeof state === 'string' ? state : state.value);
  log(`browser state ${JSON.stringify(s)}`);
  if (!s.ready || s.peers[0] !== node.id || s.received[0] !== 'bonjour navigateur' || s.received[1] !== 'bytes:1,2,3') throw new Error('browser state does not match');
  log(`PASS — browser ↔ node exchange both ways, via ${peer.via} (browser side ${s.via})`);
  code = 0;
} catch (e) {
  log(`FAIL — ${e.message}`);
} finally {
  node.leave();
  await relay.close();
  server.close();
  process.exit(code);
}
