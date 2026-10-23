// Test clients : real WebSockets (and raw TCP upgrades) to a real relay, in-process or as a child process.
import { strict as assert } from 'node:assert';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { connect as netConnect } from 'node:net';
import WebSocket from 'ws';

export const quiet = () => {};

/** A v1 room id : 43 base64url chars, like HKDF-SHA256(secret, room) on the client. */
export const roomId = () => randomBytes(32).toString('base64url');

export function connect(url, wsOptions) {
  const ws = new WebSocket(url, wsOptions);
  const queue = [];
  const waiters = [];
  const closed = new Promise((resolve) => ws.on('close', (code) => resolve(code)));
  ws.on('error', () => {});
  ws.on('message', (raw) => {
    const msg = JSON.parse(raw.toString());
    const i = waiters.findIndex((w) => w.type === msg.type);
    if (i >= 0) waiters.splice(i, 1)[0].resolve(msg);
    else queue.push(msg);
  });
  // Resolves with the HTTP status when the upgrade is refused (429…), so a refusal is assertable.
  const opened = new Promise((resolve, reject) => {
    ws.once('open', () => resolve(101));
    ws.once('unexpected-response', (req, res) => { resolve(res.statusCode); res.resume(); });
    ws.once('error', reject);
  });
  return {
    ws,
    opened,
    closed,
    send: (obj) => ws.send(typeof obj === 'string' ? obj : JSON.stringify(obj)),
    /** Resolves with the next message of `type`, or rejects after `ms`. */
    next(type, ms = 2000) {
      const i = queue.findIndex((m) => m.type === type);
      if (i >= 0) return Promise.resolve(queue.splice(i, 1)[0]);
      return new Promise((resolve, reject) => {
        const waiter = { type, resolve };
        waiters.push(waiter);
        setTimeout(() => {
          const j = waiters.indexOf(waiter);
          if (j >= 0) { waiters.splice(j, 1); reject(new Error(`no ${type} within ${ms} ms`)); }
        }, ms);
      });
    },
    /** True when nothing of `type` arrives within `ms`. */
    async silent(type, ms = 300) {
      try { await this.next(type, ms); return false; } catch { return true; }
    },
    close: () => ws.close(),
  };
}

/** Connects and says `hello` ; resolves with the client once welcomed. */
export async function join(url, hello, wsOptions) {
  const c = connect(url, wsOptions);
  assert.equal(await c.opened, 101, 'upgrade refused');
  c.send({ type: 'hello', ...hello });
  c.welcome = await c.next('welcome');
  return c;
}

/** Opens a socket and sends `first` as its first message ; resolves with the close code. */
export async function closeCodeOf(url, first, wsOptions) {
  const c = connect(url, wsOptions);
  await c.opened;
  if (first !== undefined) c.send(first);
  return c.closed;
}

/**
 * The relay still serves : two fresh v1 peers meet in a new room and one's signal reaches the other.
 * @param {string} url
 * @param {object} [extra] - added to both hellos (key) ; `wsOptions` for both sockets
 */
export async function assertServing(url, extra = {}, wsOptions) {
  const room = roomId();
  const tag = randomBytes(3).toString('hex');
  const a = await join(url, { v: 1, id: `ok-a-${tag}`, room, ...extra }, wsOptions);
  const b = await join(url, { v: 1, id: `ok-b-${tag}`, room, ...extra }, wsOptions);
  a.send({ type: 'signal', to: `ok-b-${tag}`, kind: 'offer', box: 'b2s' });
  const got = await b.next('signal');
  assert.deepEqual(got, { type: 'signal', from: `ok-a-${tag}`, kind: 'offer', box: 'b2s' });
  a.close(); b.close();
  await Promise.all([a.closed, b.closed]);
}

/**
 * Raw TCP WebSocket upgrade that then says nothing. Resolves with the HTTP status the relay
 * answered (101 or 429) ; the socket stays open on 101 until `destroy()`.
 */
export function rawUpgrade(port, { headers = {}, localAddress } = {}) {
  return new Promise((resolve) => {
    const socket = netConnect({ port, host: '127.0.0.1', localAddress });
    let buf = '';
    let done = false;
    const finish = (status) => { if (!done) { done = true; resolve({ status, socket }); } };
    socket.on('error', () => finish(0));
    socket.on('close', () => finish(0));
    socket.on('data', (chunk) => {
      buf += chunk;
      const m = /^HTTP\/1\.1 (\d{3})/.exec(buf);
      if (m) finish(Number(m[1]));
    });
    socket.on('connect', () => {
      let extra = '';
      for (const name in headers) extra += `${name}: ${headers[name]}\r\n`;
      socket.write(`GET / HTTP/1.1\r\nHost: relay\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n`
        + `Sec-WebSocket-Key: ${randomBytes(16).toString('base64')}\r\nSec-WebSocket-Version: 13\r\n${extra}\r\n`);
    });
  });
}

/** Runs bin/signal.js with `env` ; resolves once it listens. `waitLog(re)` awaits a line of its output. */
export async function spawnRelay(env) {
  const child = spawn(process.execPath, [new URL('../bin/signal.js', import.meta.url).pathname], {
    env: { ...process.env, PORT: '0', HOST: '127.0.0.1', ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let out = '';
  const waiters = [];
  const onData = (chunk) => {
    out += chunk;
    if (out.length > 1_000_000) out = out.slice(-200_000); // a flood logs a lot ; keep the tail
    for (const w of waiters.slice()) if (w.re.test(out)) { waiters.splice(waiters.indexOf(w), 1); w.resolve(out.match(w.re)); }
  };
  child.stdout.on('data', onData);
  child.stderr.on('data', onData);
  const waitLog = (re, ms = 5000) => new Promise((resolve, reject) => {
    if (re.test(out)) return resolve(out.match(re));
    waiters.push({ re, resolve });
    setTimeout(() => reject(new Error(`no ${re} in relay output within ${ms} ms :\n${out.slice(-2000)}`)), ms);
  });
  const port = Number((await waitLog(/listening wss?:\/\/127\.0\.0\.1:(\d+)/))[1]);
  const exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve(signal ?? code)));
  return {
    child,
    port,
    waitLog,
    exited,
    alive: () => child.exitCode === null && child.signalCode === null,
    /** Resident memory of the relay process, in MB (Linux). */
    async rssMb() {
      const { readFile } = await import('node:fs/promises');
      const status = await readFile(`/proc/${child.pid}/status`, 'utf8');
      return Number(/VmRSS:\s+(\d+) kB/.exec(status)[1]) / 1024;
    },
    stop() {
      child.kill('SIGTERM');
      return exited;
    },
  };
}

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
