// node ↔ node through the real relay : they meet, talk, fragment, wait, mesh, and leave cleanly.
import { strict as assert } from 'node:assert';
import { after, before, test } from 'node:test';
import { newSecret } from '../src/crypto.js';
import { platform } from '../src/node-platform.js';
import { collect, joinWith, leaveAll, meet, metered, nextMessage, nodeJoin, once, silent, sleep, startRelay, tapped, urlOf, waitFor } from './client-helpers.js';

let relay;
let url;
before(async () => { relay = await startRelay(); url = urlOf(relay); });
after(() => relay.close());

const fresh = (extra) => ({ relay: url, room: 'salon', secret: newSecret(), ...extra });

test('two peers meet directly ; strings and bytes arrive as sent, both ways', async () => {
  // a enters first on purpose : the creator is whoever's hello the relay processed first.
  const options = fresh();
  const a = nodeJoin({ ...options, iceServers: [] });
  await a.ready;
  const b = nodeJoin({ ...options, iceServers: [] });
  await b.ready;
  await waitFor(() => a.peers.length === 1 && b.peers.length === 1);
  try {
    assert.equal(a.isCreator, true);
    assert.equal(b.isCreator, false);
    assert.equal(a.max, 8);
    assert.equal(b.max, 8);
    assert.equal(a.id.length, 22);
    const pa = a.peers[0];
    const pb = b.peers[0];
    assert.equal(pa.id, b.id);
    assert.equal(pb.id, a.id);
    assert.equal(pa.via, 'direct');
    assert.equal(pb.via, 'direct');
    const got = nextMessage(pb);
    await pa.send('salut é 東京');
    assert.equal(await got, 'salut é 東京');
    const bytes = new Uint8Array([0, 1, 2, 255]);
    const gotBytes = nextMessage(pa);
    await pb.send(bytes);
    const received = await gotBytes;
    assert.ok(received instanceof Uint8Array);
    assert.deepEqual([...received], [0, 1, 2, 255]);
    const gotBuffer = nextMessage(pa);
    await pb.send(new Uint8Array([7, 8]).buffer);
    assert.deepEqual([...(await gotBuffer)], [7, 8]);
    const gotEmpty = nextMessage(pb);
    await pa.send('');
    assert.equal(await gotEmpty, '');
  } finally {
    leaveAll([a, b]);
  }
});

test('a 200 KB message crosses the 64 KB channel limit and arrives whole, in order with its neighbours', async () => {
  const [a, b] = await meet(fresh());
  try {
    const big = new Uint8Array(200 * 1024);
    for (let i = 0; i < big.length; i++) big[i] = (i * 7) & 255;
    const inbox = collect(b.peers[0], 'message');
    await a.peers[0].send('before');
    await a.peers[0].send(big);
    await a.peers[0].send('x'.repeat(70_000));
    await a.peers[0].send('after');
    await waitFor(() => inbox.length === 4, 8000, '4 messages');
    assert.equal(inbox[0], 'before');
    assert.equal(inbox[1].length, big.length);
    assert.ok(inbox[1].every((v, i) => v === ((i * 7) & 255)), 'bytes intact');
    assert.equal(inbox[2], 'x'.repeat(70_000));
    assert.equal(inbox[3], 'after');
  } finally {
    leaveAll([a, b]);
  }
});

test('maxMessageBytes : send() rejects too-big above it ; a peer sending above ours is cut', async () => {
  const [a, b] = await meet(fresh({ maxMessageBytes: 1000 }));
  try {
    await assert.rejects(a.peers[0].send(new Uint8Array(1001)), (e) => e.code === 'too-big' && e.fatal === false);
    await assert.rejects(a.peers[0].send('x'.repeat(1001)), (e) => e.code === 'too-big');
    const got = nextMessage(b.peers[0]);
    await a.peers[0].send(new Uint8Array(1000));
    assert.equal((await got).length, 1000);
    await assert.rejects(a.peers[0].send(42), (e) => e.code === 'options');
  } finally {
    leaveAll([a, b]);
  }
});

test('a peer whose messages exceed our maxMessageBytes is cut with too-big', async () => {
  const options = fresh();
  // Same room, same secret, a larger cap on the sender : it may send 2 KB, we refuse to receive it.
  const small = nodeJoin({ ...options, iceServers: [], maxMessageBytes: 500 });
  const sender = nodeJoin({ ...options, iceServers: [], maxMessageBytes: 4096 });
  await Promise.all([small.ready, sender.ready]);
  await waitFor(() => small.peers.length === 1 && sender.peers.length === 1);
  try {
    const errors = collect(small, 'error');
    const left = once(small, 'leave');
    await sender.peers[0].send(new Uint8Array(2000));
    const [peer, reason] = await left;
    assert.equal(peer.id, sender.id);
    assert.equal(reason, 'too-big');
    assert.ok(errors.some((e) => e.code === 'too-big'), 'error too-big emitted');
    await waitFor(() => sender.peers.length === 0, 5000, 'sender sees the close');
  } finally {
    leaveAll([small, sender]);
  }
});

test('a send() loop against the channel keeps the buffered amount bounded (backpressure)', async () => {
  const stats = { max: 0, sent: 0 };
  const options = fresh();
  const a = joinWith(metered(stats))({ ...options, iceServers: [] });
  const b = nodeJoin({ ...options, iceServers: [] });
  await Promise.all([a.ready, b.ready]);
  await waitFor(() => a.peers.length === 1 && b.peers.length === 1);
  try {
    let received = 0;
    b.peers[0].on('message', (m) => { received += m.length; });
    const message = new Uint8Array(64 * 1024);
    const total = 128; // 8 MB
    for (let i = 0; i < total; i++) await a.peers[0].send(message);
    await waitFor(() => received === total * message.length, 20_000, 'all bytes received');
    assert.ok(stats.sent >= total * 4, `frames sent : ${stats.sent}`);
    assert.ok(stats.max <= 1024 * 1024 + 20 * 1024, `bufferedAmount peaked at ${stats.max}`);
  } finally {
    leaveAll([a, b]);
  }
});

test('mesh of 4 : every pair linked, broadcast reaches everyone, peer.close() cuts one link only', async () => {
  const rooms = await meet(fresh(), 4);
  try {
    const ids = rooms.map((r) => r.id).sort();
    for (const r of rooms) {
      assert.deepEqual([r.id, ...r.peers.map((p) => p.id)].sort(), ids);
      assert.ok(r.peers.every((p) => p.via === 'direct'));
    }
    assert.equal(rooms.filter((r) => r.isCreator).length, 1);
    const inboxes = rooms.slice(1).map((r) => collect(r.peers.find((p) => p.id === rooms[0].id), 'message'));
    await rooms[0].broadcast('à tous');
    await waitFor(() => inboxes.every((i) => i.length === 1));
    assert.deepEqual(inboxes.map((i) => i[0]), ['à tous', 'à tous', 'à tous']);
    const [a, b, c] = rooms;
    const leftA = once(a, 'leave');
    const leftB = once(b, 'leave');
    a.peers.find((p) => p.id === b.id).close();
    assert.equal((await leftA)[0].id, b.id);
    assert.equal((await leftB)[0].id, a.id);
    assert.equal((await leftB)[1], 'closed');
    assert.equal(c.peers.length, 3);
    assert.equal(a.peers.length, 2);
  } finally {
    leaveAll(rooms);
  }
});

test('max: 2 — the third entrant gets room-full, the two inside are untouched', async () => {
  const options = fresh({ max: 2 });
  const [a, b] = await meet(options);
  try {
    const c = nodeJoin({ ...options, iceServers: [] });
    const closed = once(c, 'close');
    await assert.rejects(c.ready, (e) => e.code === 'room-full' && e.fatal === true);
    assert.equal((await closed)[0].code, 'room-full');
    await sleep(200);
    assert.equal(a.peers.length, 1);
    assert.equal(b.peers.length, 1);
    assert.equal(a.max, 2);
  } finally {
    leaveAll([a, b]);
  }
});

test('relayKey : wrong → key (fatal) ; right → in', async () => {
  const locked = await startRelay({ key: 's3cret' });
  try {
    const options = { relay: urlOf(locked), room: 'r', secret: newSecret(), iceServers: [] };
    const bad = nodeJoin({ ...options, relayKey: 'nope' });
    await assert.rejects(bad.ready, (e) => e.code === 'key' && e.fatal === true);
    const none = nodeJoin(options);
    await assert.rejects(none.ready, (e) => e.code === 'key');
    const [a, b] = await meet({ ...options, relayKey: 's3cret' });
    leaveAll([a, b]);
  } finally {
    await locked.close();
  }
});

test('same room name, another secret : different rooms on the relay, they never meet', async () => {
  const frames = [];
  const a = joinWith(tapped(frames))({ relay: url, room: 'salon', secret: newSecret(), iceServers: [] });
  const b = joinWith(tapped(frames))({ relay: url, room: 'salon', secret: newSecret(), iceServers: [] });
  await Promise.all([a.ready, b.ready]);
  try {
    assert.equal(await silent(a, 'peer', 1500), true);
    assert.equal(b.peers.length, 0);
    const hellos = frames.filter((f) => f.dir === 'out' && f.text.includes('"hello"')).map((f) => JSON.parse(f.text));
    assert.equal(hellos.length, 2);
    assert.notEqual(hellos[0].room, hellos[1].room);
    assert.match(hellos[0].room, /^[A-Za-z0-9_-]{43}$/);
    assert.ok(!frames.some((f) => f.text.includes('salon')), 'the room name never reaches the relay');
  } finally {
    leaveAll([a, b]);
  }
});

test('leave() while the welcome is in flight : ready rejects closed, nothing fires after close, no connection is opened', async () => {
  const options = fresh();
  const a = nodeJoin({ ...options, iceServers: [] });
  await a.ready;
  let connections = 0;
  // b's socket delivers every message 150 ms late : leave() lands between the hello and the welcome.
  class Slow extends platform.WebSocket {
    addEventListener(type, fn, o) { super.addEventListener(type, type === 'message' ? (e) => setTimeout(() => fn(e), 150) : fn, o); }
  }
  const counting = { WebSocket: Slow, RTCPeerConnection: class extends platform.RTCPeerConnection { constructor(c) { super(c); connections++; } } };
  const b = joinWith(counting)({ ...options, iceServers: [], connectMs: 300, handshakeMs: 300 });
  const after = [];
  try {
    const closed = once(b, 'close');
    await sleep(60);
    b.leave();
    for (const event of ['peer', 'error', 'leave', 'close']) b.on(event, () => after.push(event));
    assert.equal((await closed)[0], null);
    await assert.rejects(b.ready, (e) => e.code === 'closed');
    await sleep(1200);
    assert.deepEqual(after, []);
    assert.equal(connections, 0);
    assert.equal(a.peers.length, 0);
  } finally {
    b.leave();
    a.leave();
  }
});

test('leave() : close(null), the other side gets leave, sends reject closed', async () => {
  const [a, b] = await meet(fresh());
  const pa = a.peers[0];
  const closedA = once(a, 'close');
  const leftB = once(b, 'leave');
  a.leave();
  assert.equal((await closedA)[0], null);
  const [peer, reason] = await leftB;
  assert.equal(peer.id, a.id);
  assert.equal(reason, 'closed');
  assert.equal(pa.closed, true);
  await assert.rejects(pa.send('x'), (e) => e.code === 'closed');
  assert.deepEqual(a.peers, []);
  assert.deepEqual(b.peers, []);
  a.leave(); // idempotent
  const closedB = once(b, 'close');
  b.leave();
  assert.equal((await closedB)[0], null);
});
