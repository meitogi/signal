// Relay key list : one key per application, each with its own rate ; RELAY_KEY kept as a list of one ;
// legacy hellos exempt ; the env parsing of bin/signal.js.
import { strict as assert } from 'node:assert';
import { describe, test } from 'node:test';
import { CLOSE, createRelay, parseKeys } from '../src/relay.js';
import { assertServing, closeCodeOf, join, quiet, roomId, sleep, spawnRelay } from './helpers.js';

const NO_BAN = { banAfter: 1e6, ipConn: 1000 };
const GAME = 'game-key-0123456789';
const PAGE = 'page-key-0123456789';

async function start(options) {
  const relay = await createRelay({ port: 0, host: '127.0.0.1', log: quiet, ...NO_BAN, ...options });
  return { relay, url: `ws://127.0.0.1:${relay.port}` };
}

/** Sends `n` data frames from `a` to `b` at once ; resolves with how many `b` got. */
async function flood(a, b, to, n) {
  let received = 0;
  const count = (raw) => { if (JSON.parse(raw).type === 'data') received++; };
  b.ws.on('message', count);
  for (let i = 0; i < n; i++) a.send({ type: 'data', to, box: 'AA' });
  await sleep(300);
  b.ws.off('message', count);
  return received;
}

describe('key list', () => {
  test('each listed key is welcomed with its own grant ; wrong, missing or other-type keys close 4401', async (t) => {
    const { relay, url } = await start({
      helloMs: 400,
      keys: [{ name: 'game', key: GAME, rate: 400, burst: 800 }, { name: 'page', key: PAGE }],
    });
    t.after(() => relay.close());
    const g = await join(url, { v: 1, id: 'g1', room: roomId(), key: GAME });
    assert.deepEqual([g.welcome.rate, g.welcome.burst], [400, 800]);
    const p = await join(url, { v: 1, id: 'p1', room: roomId(), key: PAGE });
    assert.deepEqual([p.welcome.rate, p.welcome.burst], [120, 240], 'an entry without rates gets the defaults');
    for (const key of [undefined, '', 'nope', GAME + 'x', 7]) {
      assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'kx', room: roomId(), key }), CLOSE.key, `key ${key}`);
    }
    g.close(); p.close();
    await assertServing(url, { key: PAGE });
  });

  test("a key's own rate is enforced (4429 at its cap) while another key's socket keeps flowing", async (t) => {
    const { relay, url } = await start({
      keys: [{ name: 'slow', key: PAGE, rate: 10, burst: 20 }, { name: 'fast', key: GAME, rate: 1000, burst: 2000 }],
    });
    t.after(() => relay.close());
    const room = roomId();
    const slow = await join(url, { v: 1, id: 'slow', room, key: PAGE });
    const fast = await join(url, { v: 1, id: 'fast', room, key: GAME });
    const sink = await join(url, { v: 1, id: 'sink', room, key: GAME });
    const fromFast = await flood(fast, sink, 'sink', 500);
    assert.equal(fromFast, 500, 'the fast key passes 500 at once (its burst is 2000)');
    const fromSlow = await flood(slow, sink, 'sink', 30);
    // The slow socket starts its key's bucket full at its hello : 20, plus what 10/s refilled meanwhile.
    assert.ok(fromSlow >= 20 && fromSlow <= 23, `slow got ${fromSlow} of 30 through`);
    for (let i = 0; i < 40; i++) slow.send({ type: 'data', to: 'sink', box: 'AA' });
    assert.equal(await slow.closed, CLOSE.rate);
    assert.equal(await flood(fast, sink, 'sink', 300), 300, 'the other key is unaffected');
    fast.close(); sink.close();
  });

  test('the bucket before the hello is RELAY_RATE / RELAY_BURST, then the key takes over', async (t) => {
    const { relay, url } = await start({ rate: 10, burst: 20, keys: [{ name: 'big', key: GAME, rate: 1000, burst: 2000 }] });
    t.after(() => relay.close());
    const room = roomId();
    const a = await join(url, { v: 1, id: 'pre-a', room, key: GAME });
    const b = await join(url, { v: 1, id: 'pre-b', room, key: GAME });
    assert.deepEqual([a.welcome.rate, a.welcome.burst], [1000, 2000]);
    assert.equal(await flood(a, b, 'pre-b', 200), 200, 'far above the pre-hello 20 : the key bucket applies');
    a.close(); b.close();
  });

  test('`key` alone is a list of one at the default rate', async (t) => {
    const { relay, url } = await start({ key: GAME, rate: 50, burst: 70 });
    t.after(() => relay.close());
    const a = await join(url, { v: 1, id: 'one', room: roomId(), key: GAME });
    assert.deepEqual([a.welcome.rate, a.welcome.burst], [50, 70]);
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'none', room: roomId() }), CLOSE.key);
    a.close();
  });

  test('`key` and `keys` together : the relay refuses to start', async () => {
    await assert.rejects(start({ key: GAME, keys: [{ name: 'page', key: PAGE }] }), /key.*keys|keys.*key/i);
  });

  test('an open relay grants RELAY_RATE / RELAY_BURST in its welcome', async (t) => {
    const { relay, url } = await start({ rate: 30, burst: 60 });
    t.after(() => relay.close());
    const a = await join(url, { v: 1, id: 'open', room: roomId() });
    assert.deepEqual([a.welcome.rate, a.welcome.burst], [30, 60]);
    a.close();
  });

  test('legacy : a hello without `v` is admitted with no key on a keyed relay ; a keyless v1 is still 4401', async (t) => {
    const { relay, url } = await start({ legacy: true, keys: [{ name: 'game', key: GAME }] });
    t.after(() => relay.close());
    const old = await join(url, { type: 'hello', id: 'srv-1', name: 'debrid-remote', kind: 'server' });
    assert.deepEqual(old.welcome, { type: 'welcome', peers: [{ id: 'srv-1', name: 'debrid-remote', kind: 'server' }] });
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'v1', room: roomId() }), CLOSE.key);
    const v1 = await join(url, { v: 1, id: 'v1k', room: roomId(), key: GAME });
    assert.equal(v1.welcome.creator, true);
    old.close(); v1.close();
  });

  test('legacy off : a hello without `v` on a keyed relay is refused (4401 : no key)', async (t) => {
    const { relay, url } = await start({ keys: [{ name: 'game', key: GAME }] });
    t.after(() => relay.close());
    assert.equal(await closeCodeOf(url, { type: 'hello', id: 'srv-1', name: 'debrid-remote', kind: 'server' }), CLOSE.key);
  });
});

describe('parseKeys (RELAY_KEYS)', () => {
  test('entries name:key[:rate[:burst]], comma-separated, spaces around entries ignored', () => {
    assert.deepEqual(parseKeys(`game:${GAME}:400:800, page:${PAGE}:30 ,mmo:abc`), [
      { name: 'game', key: GAME, rate: 400, burst: 800 },
      { name: 'page', key: PAGE, rate: 30, burst: undefined },
      { name: 'mmo', key: 'abc', rate: undefined, burst: undefined },
    ]);
  });

  test('malformed lists throw without ever quoting a key', () => {
    const cases = [
      '', 'nokey', 'game:', ':key', 'game:k:0', 'game:k:1.5', 'game:k:10:x', 'game:k:1:2:3',
      `game:${GAME},game:${PAGE}`, `a:${GAME},b:${GAME}`, 'bad name:k', `game:${GAME},`,
    ];
    for (const text of cases) {
      assert.throws(() => parseKeys(text), (e) => {
        assert.ok(!e.message.includes(GAME) && !e.message.includes(PAGE), `the key leaked in : ${e.message}`);
        return true;
      }, `${JSON.stringify(text)} must throw`);
    }
  });
});

describe('bin/signal.js env', () => {
  test('RELAY_KEYS : the listed keys get in with their rates, the log names them, never prints them', async (t) => {
    const relay = await spawnRelay({ RELAY_KEYS: `game:${GAME}:300:600,page:${PAGE}`, RELAY_RATE: '40', RELAY_BURST: '80' });
    t.after(() => relay.stop());
    const url = `ws://127.0.0.1:${relay.port}`;
    const [line] = await relay.waitLog(/listening.*$/m);
    assert.match(line, /2 keys \(game, page\)/);
    const g = await join(url, { v: 1, id: 'g', room: roomId(), key: GAME });
    assert.deepEqual([g.welcome.rate, g.welcome.burst], [300, 600]);
    const p = await join(url, { v: 1, id: 'p', room: roomId(), key: PAGE });
    assert.deepEqual([p.welcome.rate, p.welcome.burst], [40, 80]);
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'x', room: roomId(), key: 'nope' }), CLOSE.key);
    await relay.waitLog(/hello g\S* .*key=game/);
    g.close(); p.close();
  });

  for (const [what, env, re] of [
    ['RELAY_KEY with RELAY_KEYS', { RELAY_KEY: GAME, RELAY_KEYS: `page:${PAGE}` }, /RELAY_KEY.*RELAY_KEYS|RELAY_KEYS.*RELAY_KEY/],
    ['a malformed RELAY_KEYS', { RELAY_KEYS: `page:${PAGE}:fast` }, /RELAY_KEYS/],
  ]) {
    test(`${what} : exit 1 with a message, the key not printed`, async () => {
      const { spawn } = await import('node:child_process');
      const child = spawn(process.execPath, [new URL('../bin/signal.js', import.meta.url).pathname], {
        env: { ...process.env, PORT: '0', HOST: '127.0.0.1', ...env },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '';
      child.stdout.on('data', (c) => { out += c; });
      child.stderr.on('data', (c) => { out += c; });
      const code = await new Promise((resolve) => child.once('exit', resolve));
      assert.equal(code, 1, out);
      assert.match(out, re);
      assert.ok(!out.includes(GAME) && !out.includes(PAGE), `a key leaked : ${out}`);
    });
  }

  test('RELAY_KEY alone still works : one key, the default rate', async (t) => {
    const relay = await spawnRelay({ RELAY_KEY: GAME });
    t.after(() => relay.stop());
    const url = `ws://127.0.0.1:${relay.port}`;
    const a = await join(url, { v: 1, id: 'a', room: roomId(), key: GAME });
    assert.deepEqual([a.welcome.rate, a.welcome.burst], [120, 240]);
    assert.equal(await closeCodeOf(url, { type: 'hello', v: 1, id: 'b', room: roomId() }), CLOSE.key);
    a.close();
  });
});
