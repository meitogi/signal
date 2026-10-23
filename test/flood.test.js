// Silent-connection floods, X-Forwarded-For trust and IP bans, against bin/signal.js as a real process :
// every refusal is an HTTP 429 at `upgrade`, memory stays bounded, a legit client still gets in.
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { CLOSE } from '../src/relay.js';
import { assertServing, closeCodeOf, connect, rawUpgrade, roomId, sleep, spawnRelay } from './helpers.js';

const FLOOD = 5000;
const BATCH = 500;
const RSS_BUDGET_MB = 64;

/** Opens `count` silent upgrades (`headersOf(i)` per socket) ; returns how many got each status, and the sockets. */
async function flood(port, count, headersOf = () => ({})) {
  const statuses = {};
  const sockets = [];
  for (let start = 0; start < count; start += BATCH) {
    const batch = [];
    for (let i = start; i < Math.min(count, start + BATCH); i++) batch.push(rawUpgrade(port, { headers: headersOf(i) }));
    for (const { status, socket } of await Promise.all(batch)) {
      statuses[status] = (statuses[status] ?? 0) + 1;
      sockets.push(socket);
    }
  }
  return { statuses, sockets };
}

const xff = (i) => `10.${(i >> 16) & 255}.${(i >> 8) & 255}.${i & 255}`;

test(`${FLOOD} silent sockets from one IP : ${10} in, the rest 429 then banned ; memory bounded ; another IP still served`, async (t) => {
  const relay = await spawnRelay({ RELAY_HELLO_MS: '60000' });
  t.after(relay.stop);
  const rss0 = await relay.rssMb();
  const { statuses, sockets } = await flood(relay.port, FLOOD);
  t.after(() => { for (const s of sockets) s.destroy(); });
  assert.deepEqual(statuses, { 101: 10, 429: FLOOD - 10 }, 'RELAY_IP_CONN=10 sockets, then 429 before any WebSocket');
  await relay.waitLog(/ban 127\.0\.0\.1 for 600 s/);
  const grown = (await relay.rssMb()) - rss0;
  assert.ok(grown < RSS_BUDGET_MB, `RSS grew ${grown.toFixed(1)} MB`);
  assert.ok(relay.alive());
  // 127.0.0.1 is banned : a client from 127.0.0.2 (another loopback IP) is served.
  await assertServing(`ws://127.0.0.1:${relay.port}`, {}, { localAddress: '127.0.0.2' });
  const banned = connect(`ws://127.0.0.1:${relay.port}`);
  assert.equal(await banned.opened, 429, 'the banned IP is refused even with a legit client');
});

test(`${FLOOD} silent sockets from many X-Forwarded-For IPs behind a trusted proxy : RELAY_PENDING_MAX in, the rest 429, no ban ; the legit client is 429 cleanly, then gets in`, async (t) => {
  const relay = await spawnRelay({ TRUST_PROXY: '127.0.0.1', RELAY_HELLO_MS: '10000' });
  t.after(relay.stop);
  const url = `ws://127.0.0.1:${relay.port}`;
  const rss0 = await relay.rssMb();
  const t0 = Date.now();
  const { statuses, sockets } = await flood(relay.port, FLOOD, (i) => ({ 'X-Forwarded-For': xff(i) }));
  t.after(() => { for (const s of sockets) s.destroy(); });
  assert.ok(Date.now() - t0 < 9000, 'the flood fits in the hello delay (else pending drains during it)');
  assert.deepEqual(statuses, { 101: 200, 429: FLOOD - 200 }, 'RELAY_PENDING_MAX=200 pending sockets, then 429');
  const grown = (await relay.rssMb()) - rss0;
  assert.ok(grown < RSS_BUDGET_MB, `RSS grew ${grown.toFixed(1)} MB`);

  const legit = { headers: { 'X-Forwarded-For': '192.0.2.10' } };
  const early = connect(url, legit);
  assert.equal(await early.opened, 429, 'while pending is full, a newcomer gets a clean 429');
  // The silent ones hit the hello delay (4401) and are cut after the close grace : pending frees up.
  let served = false;
  for (const end = Date.now() + 15_000; !served && Date.now() < end; await sleep(250)) {
    const probe = connect(url, legit);
    served = (await probe.opened) === 101;
    probe.ws.terminate();
  }
  assert.ok(served, 'pending drained after the hello delay');
  await assertServing(url, {}, legit);
  assert.ok(!/ ban /.test((await relay.waitLog(/[\s\S]*/))[0]), 'one fault per IP : nobody banned');
  assert.ok(relay.alive());
});

test('X-Forwarded-For from an untrusted source is ignored : the socket IP counts', async (t) => {
  const relay = await spawnRelay({ TRUST_PROXY: '10.0.0.0/8' });
  t.after(relay.stop);
  const { statuses, sockets } = await flood(relay.port, 50, (i) => ({ 'X-Forwarded-For': xff(i) }));
  t.after(() => { for (const s of sockets) s.destroy(); });
  assert.deepEqual(statuses, { 101: 10, 429: 40 }, 'all 50 counted as 127.0.0.1 : RELAY_IP_CONN applies');
  await assertServing(`ws://127.0.0.1:${relay.port}`, {}, { localAddress: '127.0.0.2' });
});

test('RELAY_CONN_MAX : past it, a newcomer from a fresh IP gets 429 ; the joined peers keep talking', async (t) => {
  const relay = await spawnRelay({ TRUST_PROXY: '127.0.0.1', RELAY_CONN_MAX: '4' });
  t.after(relay.stop);
  const url = `ws://127.0.0.1:${relay.port}`;
  const room = roomId();
  const peers = [];
  for (let i = 0; i < 4; i++) {
    const c = connect(url, { headers: { 'X-Forwarded-For': xff(i) } });
    assert.equal(await c.opened, 101);
    c.send({ type: 'hello', v: 1, id: `p${i}`, room });
    await c.next('welcome');
    peers.push(c);
  }
  const extra = connect(url, { headers: { 'X-Forwarded-For': xff(99) } });
  assert.equal(await extra.opened, 429);
  peers[0].send({ type: 'signal', to: 'p3', kind: 'offer', box: 'still' });
  assert.equal((await peers[3].next('signal')).box, 'still');
  peers[3].close();
  await peers[3].closed;
  await sleep(50);
  const back = connect(url, { headers: { 'X-Forwarded-For': xff(99) } });
  assert.equal(await back.opened, 101, 'a slot freed is a slot available');
  back.close();
  for (const p of peers) p.close();
});

test('ban escalation : RELAY_BAN_AFTER faults ban the IP for RELAY_BAN_MS, a repeat doubles it', async (t) => {
  const relay = await spawnRelay({ RELAY_BAN_AFTER: '2', RELAY_BAN_MS: '800' });
  t.after(relay.stop);
  const url = `ws://127.0.0.1:${relay.port}`;
  const from = { localAddress: '127.0.0.3' };

  async function offendAndTimeBan() {
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'x', room: 'not-a-hash' }, from), CLOSE.badHello);
    assert.equal(await closeCodeOf(url, { type: 'ping' }, from), CLOSE.key);
    const t0 = Date.now();
    const first = connect(url, from);
    assert.equal(await first.opened, 429, 'banned right after the 2nd fault');
    for (;;) {
      await sleep(40);
      const c = connect(url, from);
      const status = await c.opened;
      if (status === 101) { c.ws.terminate(); return Date.now() - t0; }
    }
  }

  const ban1 = await offendAndTimeBan();
  assert.ok(ban1 >= 700 && ban1 < 1300, `first ban ${ban1} ms`);
  await relay.waitLog(/ban 127\.0\.0\.3 for \d+ s \(level 1\)/);
  const ban2 = await offendAndTimeBan();
  assert.ok(ban2 >= 1500 && ban2 < 2300, `second ban ${ban2} ms (doubled)`);
  await relay.waitLog(/level 2/);
  // While 127.0.0.3 was banned, other IPs were never affected.
  await assertServing(url, {}, { localAddress: '127.0.0.4' });
});
