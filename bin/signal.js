#!/usr/bin/env node
// `npx @meitogi/signal` — runs the relay from the environment. Variables : README.md / CONTRACT.md.
import { readFileSync, watchFile } from 'node:fs';
import { createRelay } from '../src/relay.js';

const env = process.env;

function envInt(name, fallback) {
  const value = env[name];
  if (!value) return fallback;
  const n = Number(value);
  if (!Number.isInteger(n) || n < 0) {
    console.error(`@meitogi/signal : ${name} must be a non-negative integer, got ${JSON.stringify(value)}`);
    process.exit(1);
  }
  return n;
}

const readTls = () => ({ cert: readFileSync(env.TLS_CERT), key: readFileSync(env.TLS_KEY) });
const tls = env.TLS_CERT && env.TLS_KEY ? readTls() : undefined;

// A renewed certificate (lego, certbot…) is written over the same files : re-read them on
// SIGHUP, or when they change. Rooms and open sockets are untouched ; a half-written pair
// fails to load and the previous certificate stays until the next change. The SIGHUP handler
// is installed before listening : a SIGHUP right at startup must not hit the default (exit).
let relay = null;
const reload = (why) => {
  if (relay === null) return console.log(`tls reload skipped (${why}) : not listening yet, the files are read at start`);
  try {
    relay.setTls(readTls());
    console.log(`tls reloaded (${why})`);
  } catch (e) {
    console.error(`tls reload failed (${why}), keeping the previous certificate : ${e.message}`);
  }
};
if (tls) process.on('SIGHUP', () => reload('SIGHUP'));

createRelay({
  port: envInt('PORT', 8787),
  host: env.HOST || '0.0.0.0',
  key: env.RELAY_KEY || '',
  tls,
  roomMax: envInt('RELAY_ROOM_MAX', 8),
  maxBytes: envInt('RELAY_MAX_BYTES', 16384),
  helloMs: envInt('RELAY_HELLO_MS', 5000),
  idleMs: envInt('RELAY_IDLE_MS', 30_000),
  rate: envInt('RELAY_RATE', 120),
  burst: envInt('RELAY_BURST', 240),
  offersPerMin: envInt('RELAY_OFFERS_PER_MIN', 6),
  ipConn: envInt('RELAY_IP_CONN', 10),
  pendingMax: envInt('RELAY_PENDING_MAX', 200),
  connMax: envInt('RELAY_CONN_MAX', 2000),
  banAfter: envInt('RELAY_BAN_AFTER', 5),
  banMs: envInt('RELAY_BAN_MS', 600_000),
  trustProxy: env.TRUST_PROXY || '',
  legacy: env.RELAY_LEGACY === '1',
  logData: env.RELAY_LOG_DATA === '1',
}).then((started) => {
  relay = started;
  if (!tls) return;
  const interval = envInt('TLS_WATCH_MS', 60_000);
  if (interval > 0) {
    for (const file of [env.TLS_CERT, env.TLS_KEY]) {
      watchFile(file, { interval }, (now, before) => { if (now.mtimeMs !== before.mtimeMs) reload('file changed'); });
    }
  }
}).catch((e) => {
  console.error('@meitogi/signal :', e.message);
  process.exit(1);
});
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => process.exit(0));
