// RELAY_LEGACY=1 : the old open protocol (the pre-v1 signal-relay that debrid-remote speaks) in room "".
import { strict as assert } from 'node:assert';
import { after, before, describe, test } from 'node:test';
import { CLOSE, createRelay } from '../src/relay.js';
import { assertServing, closeCodeOf, join, quiet, roomId, sleep } from './helpers.js';

describe('legacy mode', () => {
  let relay;
  let url;
  before(async () => {
    relay = await createRelay({ port: 0, host: '127.0.0.1', log: quiet, legacy: true, idleMs: 200, banAfter: 1e6, ipConn: 1000 });
    url = `ws://127.0.0.1:${relay.port}`;
  });
  after(() => relay.close());

  test('old hello → welcome with {id, name, kind} lists, as debrid-remote reads them', async () => {
    const srv = await join(url, { id: 'srv-1', name: 'debrid-remote', kind: 'server', ts: 1 });
    assert.deepEqual(srv.welcome, { type: 'welcome', peers: [{ id: 'srv-1', name: 'debrid-remote', kind: 'server' }] });
    const web = await join(url, { id: 'web-1', name: 'debrid-remote-web', kind: 'client', ts: 2 });
    assert.ok(web.welcome.peers.some((p) => p.kind === 'server' && p.name === 'debrid-remote'));
    const anon = await join(url, { id: 'abcdef123' });
    assert.deepEqual(anon.welcome.peers.at(-1), { id: 'abcdef123', name: 'abcdef', kind: 'client' });
    srv.close(); web.close(); anon.close();
  });

  test('signal forwarded with every field, from stamped ; pings answered', async () => {
    const srv = await join(url, { id: 'srv-2', name: 'debrid-remote', kind: 'server' });
    const web = await join(url, { id: 'web-2', name: 'debrid-remote-web', kind: 'client' });
    web.send({ type: 'signal', from: 'srv-2', to: 'srv-2', kind: 'offer', sdp: '{"type":"offer"}', ts: 5 });
    assert.deepEqual(await srv.next('signal'), { type: 'signal', from: 'web-2', to: 'srv-2', kind: 'offer', sdp: '{"type":"offer"}', ts: 5 });
    srv.send({ type: 'signal', from: 'srv-2', to: 'web-2', kind: 'answer', sdp: 'a', ts: 6 });
    assert.equal((await web.next('signal')).sdp, 'a');
    web.send({ type: 'ping', ts: 9 });
    assert.equal((await web.next('pong')).ts, 9);
    srv.close(); web.close();
  });

  test('a legacy client next to a v1 client : they never see nor reach each other', async () => {
    const room = roomId();
    const old = await join(url, { id: 'old-1', name: 'debrid-remote', kind: 'server' });
    const v1 = await join(url, { v: 1, id: 'v1-1', room });
    const v1b = await join(url, { v: 1, id: 'old-1', room }); // same id as the legacy peer : another room
    assert.deepEqual(v1.welcome.peers, [{ id: 'v1-1' }]);
    assert.deepEqual(v1b.welcome.peers, [{ id: 'v1-1' }, { id: 'old-1' }]);
    old.send({ type: 'signal', to: 'v1-1', kind: 'offer', sdp: 'x' });
    v1.send({ type: 'signal', to: 'old-1', kind: 'offer', box: 'y' });
    assert.equal(await v1.silent('signal'), true, 'legacy → v1 never delivered');
    assert.equal((await v1b.next('signal')).box, 'y', 'v1 → "old-1" reaches the v1 peer of that id');
    assert.equal(await old.silent('signal'), true, 'v1 → legacy never delivered');
    assert.deepEqual((await old.next('peers')).list.map((p) => p.id), ['old-1'], 'its own arrival');
    assert.equal(await old.silent('peers'), true, 'v1 arrivals are not announced to the legacy room');
    old.close(); v1.close(); v1b.close();
    await assertServing(url);
  });

  test('legacy peers are not idled (debrid-remote server sends nothing after hello), v1 ones are', async () => {
    const old = await join(url, { id: 'srv-idle', name: 'debrid-remote', kind: 'server' });
    const v1 = await join(url, { v: 1, id: 'v1-idle', room: roomId() });
    assert.equal(await v1.closed, CLOSE.idle);
    await sleep(300);
    assert.equal(old.ws.readyState, old.ws.OPEN);
    old.close();
  });

  test('legacy room has no RELAY_ROOM_MAX : more than 8 old clients fit', async () => {
    const peers = [];
    for (let i = 0; i < 12; i++) peers.push(await join(url, { id: `many-${i}`, kind: 'client' }));
    assert.equal(peers.at(-1).welcome.peers.length, 12);
    for (const p of peers) p.close();
  });

  test('legacy offers are capped too ; legacy has no data forwarding', async () => {
    const a = await join(url, { id: 'lo-a' });
    const b = await join(url, { id: 'lo-b' });
    a.send({ type: 'data', to: 'lo-b', box: 'x' });
    assert.equal(await b.silent('data'), true);
    for (let i = 0; i < 6; i++) a.send({ type: 'signal', to: 'lo-b', kind: 'offer', sdp: `${i}` });
    for (let i = 0; i < 6; i++) await b.next('signal');
    a.send({ type: 'signal', to: 'lo-b', kind: 'offer', sdp: '7' });
    assert.equal(await a.closed, CLOSE.rate);
    b.close();
    await assertServing(url);
  });

  test('bad legacy hello → 4400 ; id taken → 4409', async () => {
    assert.equal(await closeCodeOf(url, { type: 'hello', id: 'x'.repeat(65) }), CLOSE.badHello);
    assert.equal(await closeCodeOf(url, { type: 'hello', id: 'ok', name: 7 }), CLOSE.badHello);
    const a = await join(url, { id: 'taken' });
    assert.equal(await closeCodeOf(url, { type: 'hello', id: 'taken' }), CLOSE.idTaken);
    a.close();
    await assertServing(url);
  });
});

test('legacy off : an old hello is refused 4400', async (t) => {
  const relay = await createRelay({ port: 0, host: '127.0.0.1', log: quiet });
  t.after(() => relay.close());
  const url = `ws://127.0.0.1:${relay.port}`;
  assert.equal(await closeCodeOf(url, { type: 'hello', id: 'srv', name: 'debrid-remote', kind: 'server' }), CLOSE.badHello);
  await assertServing(url);
});
