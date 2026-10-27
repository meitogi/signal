// v1 protocol : every hostile input gets its close code, and the relay keeps serving legit peers.
import { strict as assert } from 'node:assert';
import { after, before, describe, test } from 'node:test';
import { CLOSE, createRelay } from '../src/relay.js';
import { assertServing, closeCodeOf, connect, join, quiet, roomId, sleep } from './helpers.js';

const KEY = 'k3y-for-tests';
// Bans and per-IP caps are the flood suite's subject : here every test comes from 127.0.0.1.
const NO_BAN = { banAfter: 1e6, ipConn: 1000 };

async function start(options) {
  const relay = await createRelay({ port: 0, host: '127.0.0.1', log: quiet, ...NO_BAN, ...options });
  return { relay, url: `ws://127.0.0.1:${relay.port}` };
}

describe('open relay', () => {
  let relay;
  let url;
  before(async () => ({ relay, url } = await start({ helloMs: 400 })));
  after(() => relay.close());

  test('hello → welcome {peers, max, creator} ; peers broadcast with ids only', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'a1', room, max: 4 });
    assert.deepEqual(a.welcome, { type: 'welcome', peers: [{ id: 'a1' }], max: 4, creator: true, rate: 120, burst: 240 });
    await a.next('peers');
    const b = await join(url, { v: 1, id: 'b1', room });
    assert.deepEqual(b.welcome, { type: 'welcome', peers: [{ id: 'a1' }, { id: 'b1' }], max: 4, creator: false, rate: 120, burst: 240 });
    assert.deepEqual((await a.next('peers')).list, [{ id: 'a1' }, { id: 'b1' }]);
    b.close();
    assert.deepEqual((await a.next('peers')).list, [{ id: 'a1' }]);
    a.close();
  });

  test('rooms are isolated : peers, signal and data never cross', async () => {
    const a = await join(url, { v: 1, id: 'iso-a', room: roomId() });
    const b = await join(url, { v: 1, id: 'iso-b', room: roomId() });
    assert.deepEqual(b.welcome.peers, [{ id: 'iso-b' }]);
    a.send({ type: 'signal', to: 'iso-b', kind: 'offer', box: 'x' });
    a.send({ type: 'data', to: 'iso-b', box: 'AAAA' });
    a.send({ type: 'data', box: 'BBBB' });
    assert.equal(await b.silent('signal'), true);
    assert.equal(await b.silent('data'), true);
    a.close(); b.close();
  });

  test('signal : from stamped by the relay, only {from, kind, box} forwarded', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'sa', room });
    const b = await join(url, { v: 1, id: 'sb', room });
    a.send({ type: 'signal', from: 'sb', to: 'sb', kind: 'answer', box: 'Ym94', extra: 1 });
    assert.deepEqual(await b.next('signal'), { type: 'signal', from: 'sa', kind: 'answer', box: 'Ym94' });
    for (const bad of [{ kind: 'hack', box: 'x' }, { kind: 'ice' }, { kind: 'ice', box: 7 }, { box: 'x' }]) {
      a.send({ type: 'signal', to: 'sb', ...bad });
    }
    assert.equal(await b.silent('signal'), true, 'bad kind / box dropped');
    a.close(); b.close();
  });

  test('data : unicast, broadcast to the others, from stamped, box intact', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'da', room });
    const b = await join(url, { v: 1, id: 'db', room });
    const c = await join(url, { v: 1, id: 'dc', room });
    a.send({ type: 'data', to: 'dc', from: 'db', box: 'Y2lwaGVy' });
    assert.deepEqual(await c.next('data'), { type: 'data', from: 'da', box: 'Y2lwaGVy' });
    assert.equal(await b.silent('data'), true);
    a.send({ type: 'data', box: 'YWxs' });
    assert.equal((await b.next('data')).box, 'YWxs');
    assert.equal((await c.next('data')).box, 'YWxs');
    assert.equal(await a.silent('data'), true);
    a.send({ type: 'data', box: 42 });
    assert.equal(await b.silent('data'), true, 'a box that is not a string is dropped');
    a.close(); b.close(); c.close();
  });

  test('block : nothing more from the blocked peer, the others still get through', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'ba', room });
    const bad = await join(url, { v: 1, id: 'bx', room });
    const c = await join(url, { v: 1, id: 'bc', room });
    a.send({ type: 'block', id: 'bx' });
    await sleep(50);
    bad.send({ type: 'signal', to: 'ba', kind: 'offer', box: 'x' });
    bad.send({ type: 'data', to: 'ba', box: 'x' });
    bad.send({ type: 'data', box: 'all' });
    assert.equal(await a.silent('signal'), true);
    assert.equal(await a.silent('data'), true);
    assert.equal((await c.next('data')).box, 'all', 'only the blocker stops hearing it');
    c.send({ type: 'signal', to: 'ba', kind: 'offer', box: 'c' });
    assert.equal((await a.next('signal')).from, 'bc');
    a.close(); bad.close(); c.close();
    await assertServing(url);
  });

  test('ping → pong with ts and server clock', async () => {
    const a = await join(url, { v: 1, id: 'p1', room: roomId() });
    a.send({ type: 'ping', ts: 123 });
    const pong = await a.next('pong');
    assert.equal(pong.ts, 123);
    assert.ok(Math.abs(pong.srv - Date.now()) < 5000);
    a.close();
  });

  test('bad hellos close 4400 : room format, version, id, max', async () => {
    const room = roomId();
    const bads = [
      { v: 1, id: 'x', room: 'partie-du-soir' }, // a name, not a hash
      { v: 1, id: 'x', room: room.slice(1) }, // 42 chars
      { v: 1, id: 'x', room: `${room.slice(1)}+` }, // not base64url
      { v: 1, id: 'x' },
      { v: 2, id: 'x', room },
      { v: '1', id: 'x', room },
      { id: 'x', room }, // old hello, legacy off
      { v: 1, id: 'x'.repeat(65), room },
      { v: 1, id: '', room },
      { v: 1, room },
      { v: 1, id: 'x', room, max: 1 },
      { v: 1, id: 'x', room, max: 2.5 },
      { v: 1, id: 'x', room, max: '4' },
    ];
    for (const hello of bads) assert.equal(await closeCodeOf(url, { type: 'hello', ...hello }), CLOSE.badHello, JSON.stringify(hello));
    await assertServing(url);
  });

  test('non-hello first message, or not JSON, closes 4401', async () => {
    assert.equal(await closeCodeOf(url, { type: 'ping', ts: 1 }), CLOSE.key);
    assert.equal(await closeCodeOf(url, { type: 'signal', to: 'x', kind: 'offer', box: 'x' }), CLOSE.key);
    assert.equal(await closeCodeOf(url, 'not json'), CLOSE.key);
    await assertServing(url);
  });

  test('a late hello : the socket is closed 4401 at helloMs, the hello is never read', async () => {
    const c = connect(url);
    await c.opened;
    const t0 = Date.now();
    setTimeout(() => { if (c.ws.readyState === c.ws.OPEN) c.send({ type: 'hello', v: 1, id: 'late', room: roomId() }); }, 600);
    assert.equal(await c.closed, CLOSE.key);
    const elapsed = Date.now() - t0;
    assert.ok(elapsed >= 350 && elapsed < 1500, `closed after ${elapsed} ms`);
    assert.equal(await c.silent('welcome', 50), true);
    await assertServing(url);
  });

  test('room full at max: 2 → 4403 ; it frees up when someone leaves', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'f1', room, max: 2 });
    const b = await join(url, { v: 1, id: 'f2', room });
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'f3', room }), CLOSE.roomFull);
    b.close();
    await a.next('peers');
    await a.next('peers');
    const c = await join(url, { v: 1, id: 'f3', room });
    assert.equal(c.welcome.max, 2);
    a.close(); c.close();
    await assertServing(url);
  });

  test('max: 500 is clamped to roomMax (8) ; a later max is ignored', async () => {
    const room = roomId();
    const peers = [await join(url, { v: 1, id: 'm0', room, max: 500 })];
    assert.equal(peers[0].welcome.max, 8);
    const second = await join(url, { v: 1, id: 'm1', room, max: 3 });
    assert.equal(second.welcome.max, 8, 'a joiner cannot lower max');
    assert.equal(second.welcome.creator, false);
    peers.push(second);
    for (let i = 2; i < 8; i++) peers.push(await join(url, { v: 1, id: `m${i}`, room, max: 500 }));
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'm8', room }), CLOSE.roomFull);
    for (const p of peers) p.close();
    await assertServing(url);
  });

  test('a room created with a small max cannot be raised by a joiner', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'r0', room, max: 2 });
    const b = await join(url, { v: 1, id: 'r1', room, max: 8 });
    assert.equal(b.welcome.max, 2);
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'r2', room, max: 8 }), CLOSE.roomFull);
    a.close(); b.close();
    await assertServing(url);
  });

  test('a room is deleted when empty : the next hello creates it afresh', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'e0', room, max: 2 });
    a.close();
    await a.closed;
    await sleep(50);
    const b = await join(url, { v: 1, id: 'e1', room, max: 5 });
    assert.deepEqual([b.welcome.creator, b.welcome.max], [true, 5]);
    b.close();
  });

  test('id taken in a room → 4409 ; the same id in another room is fine', async () => {
    const room = roomId();
    const a = await join(url, { v: 1, id: 'dup', room });
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'dup', room }), CLOSE.idTaken);
    const c = await join(url, { v: 1, id: 'dup', room: roomId() });
    a.close(); c.close();
    await assertServing(url);
  });

  test('/healthz answers 200, any other path 426', async () => {
    assert.equal((await fetch(`http://127.0.0.1:${relay.port}/healthz`)).status, 200);
    assert.equal((await fetch(`http://127.0.0.1:${relay.port}/`)).status, 426);
  });
});

describe('private relay (RELAY_KEY)', () => {
  let relay;
  let url;
  before(async () => ({ relay, url } = await start({ key: KEY, helloMs: 400 })));
  after(() => relay.close());

  test('the right key in the hello is welcomed', async () => {
    const a = await join(url, { v: 1, id: 'k1', room: roomId(), key: KEY });
    assert.equal(a.welcome.type, 'welcome');
    a.close();
  });

  test('hello without key, or a wrong one, closes 4401 without a welcome', async () => {
    for (const key of [undefined, 'nope', KEY + 'x', '', 7]) {
      const c = connect(url);
      await c.opened;
      c.send({ type: 'hello', v: 1, id: 'k2', room: roomId(), key });
      assert.equal(await c.closed, CLOSE.key, `key ${JSON.stringify(key)}`);
      assert.equal(await c.silent('welcome', 10), true);
    }
    await assertServing(url, { key: KEY });
  });

  test('silence closes 4401 after the hello delay', async () => {
    const c = connect(url);
    await c.opened;
    const t0 = Date.now();
    assert.equal(await c.closed, CLOSE.key);
    const elapsed = Date.now() - t0;
    assert.ok(elapsed >= 350 && elapsed < 1500, `closed after ${elapsed} ms`);
  });

  test('the default hello delay is 5 s', async (t) => {
    const { relay: r, url: u } = await start({ key: KEY });
    t.after(() => r.close());
    const c = connect(u);
    await c.opened;
    const t0 = Date.now();
    const code = await c.closed;
    const elapsed = Date.now() - t0;
    assert.equal(code, CLOSE.key);
    assert.ok(elapsed >= 4900 && elapsed < 7000, `closed after ${elapsed} ms`);
  });
});

describe('limits', () => {
  test('a message over maxBytes closes the socket (1009), smaller ones pass', async (t) => {
    const { relay, url } = await start({ maxBytes: 1024 });
    t.after(() => relay.close());
    const room = roomId();
    const a = await join(url, { v: 1, id: 'big-a', room });
    const b = await join(url, { v: 1, id: 'big-b', room });
    a.send({ type: 'data', to: 'big-b', box: 'x'.repeat(900) });
    assert.equal((await b.next('data')).box.length, 900);
    a.send({ type: 'data', to: 'big-b', box: 'x'.repeat(1100) });
    assert.equal(await a.closed, 1009);
    assert.equal(await b.silent('data'), true);
    b.close();
    await assertServing(url);
  });

  test('rate : the burst passes, the excess is dropped, sustained flooding closes 4429', async (t) => {
    const { relay, url } = await start({ rate: 10, burst: 20 });
    t.after(() => relay.close());
    const room = roomId();
    const a = await join(url, { v: 1, id: 'fl-a', room }); // the hello took one token
    const b = await join(url, { v: 1, id: 'fl-b', room });
    let received = 0;
    b.ws.on('message', (raw) => { if (JSON.parse(raw).type === 'data') received++; });
    for (let i = 0; i < 30; i++) a.send({ type: 'data', to: 'fl-b', box: 'AA' });
    await sleep(300);
    assert.ok(received >= 19 && received <= 23, `received ${received} of 30`);
    assert.equal(a.ws.readyState, a.ws.OPEN, 'a short excess only drops');
    for (let i = 0; i < 40; i++) a.send({ type: 'data', to: 'fl-b', box: 'AA' });
    assert.equal(await a.closed, CLOSE.rate);
    b.close();
    await assertServing(url);
  });

  test('offers : 6 per minute to one peer pass, the 7th closes 4429 ; answers / ice and other targets uncounted', async (t) => {
    const { relay, url } = await start();
    t.after(() => relay.close());
    const room = roomId();
    const a = await join(url, { v: 1, id: 'of-a', room });
    const b = await join(url, { v: 1, id: 'of-b', room });
    const c = await join(url, { v: 1, id: 'of-c', room });
    for (let i = 0; i < 6; i++) a.send({ type: 'signal', to: 'of-b', kind: 'offer', box: `o${i}` });
    for (let i = 0; i < 20; i++) a.send({ type: 'signal', to: 'of-b', kind: 'ice', box: `i${i}` });
    for (let i = 0; i < 6; i++) a.send({ type: 'signal', to: 'of-c', kind: 'offer', box: `c${i}` });
    for (let i = 0; i < 6; i++) assert.equal((await b.next('signal')).box, `o${i}`);
    for (let i = 0; i < 20; i++) assert.equal((await b.next('signal')).box, `i${i}`);
    for (let i = 0; i < 6; i++) assert.equal((await c.next('signal')).box, `c${i}`);
    assert.equal(a.ws.readyState, a.ws.OPEN);
    a.send({ type: 'signal', to: 'of-b', kind: 'offer', box: 'o7' });
    assert.equal(await a.closed, CLOSE.rate);
    assert.equal(await b.silent('signal'), true, 'the 7th offer is not forwarded');
    b.close(); c.close();
    await assertServing(url);
  });

  test('idle : a v1 peer silent for idleMs after its hello is closed 4408 ; pings keep it in', async (t) => {
    const { relay, url } = await start({ idleMs: 1000 });
    t.after(() => relay.close());
    const room = roomId();
    const quietPeer = await join(url, { v: 1, id: 'idle-q', room });
    const talker = await join(url, { v: 1, id: 'idle-t', room });
    const pinger = setInterval(() => talker.send({ type: 'ping', ts: 0 }), 100);
    t.after(() => clearInterval(pinger)); // even when an assertion throws : a live interval holds the file open
    const t0 = Date.now();
    assert.equal(await quietPeer.closed, CLOSE.idle);
    const elapsed = Date.now() - t0;
    assert.ok(elapsed >= 900 && elapsed < 3000, `idled after ${elapsed} ms`);
    await sleep(1200);
    assert.equal(talker.ws.readyState, talker.ws.OPEN, 'the pinging peer stays');
    talker.close();
  });
});
