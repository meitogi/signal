// connect: 'manual' : nobody links on entry ; `members` says who is there, `room.connect(id)` links one
// pair on demand, crossing connects make one link, and the relay fallback still applies.
import { strict as assert } from 'node:assert';
import { after, before, test } from 'node:test';
import { newSecret } from '../src/crypto.js';
import { brokenIce, collect, joinWith, leaveAll, nextMessage, nodeJoin, once, silent, sleep, startRelay, tapped, urlOf, waitFor } from './client-helpers.js';

let relay;
let url;
before(async () => { relay = await startRelay(); url = urlOf(relay); });
after(() => relay.close());

const fresh = (extra) => ({ relay: url, room: 'salon', secret: newSecret(), iceServers: [], connect: 'manual', ...extra });

/** Joins `n` manual rooms one after the other and waits until each lists the others. */
async function enter(options, n, joinRoom = nodeJoin) {
  const rooms = [];
  for (let i = 0; i < n; i++) {
    const r = joinRoom(options);
    await r.ready;
    rooms.push(r);
  }
  await waitFor(() => rooms.every((r) => r.members.length === n - 1), 5000, `${n} members listed`);
  return rooms;
}

const offersIn = (log) => log.filter((f) => f.dir === 'out' && f.text.includes('"kind":"offer"')).length;

test('manual : nobody links on entry ; members lists the others, not oneself', async () => {
  const [page, bot1, bot2] = await enter(fresh(), 3);
  try {
    assert.deepEqual(page.members.sort(), [bot1.id, bot2.id].sort());
    assert.deepEqual(bot1.members.sort(), [page.id, bot2.id].sort());
    assert.ok(!bot2.members.includes(bot2.id));
    await sleep(500);
    assert.equal(page.peers.length + bot1.peers.length + bot2.peers.length, 0);
  } finally {
    leaveAll([page, bot1, bot2]);
  }
});

test('manual : connect(id) links that pair only, both ways, and the payload arrives intact', async () => {
  const [page, bot1, bot2] = await enter(fresh(), 3);
  try {
    const arrived = once(bot1, 'peer');
    const peer = await page.connect(bot1.id);
    assert.equal(peer.id, bot1.id);
    assert.equal(peer.via, 'direct');
    const [back] = await arrived;
    assert.equal(back.id, page.id);
    const got = nextMessage(back);
    await peer.send('ordre : avance 3 é');
    assert.equal(await got, 'ordre : avance 3 é');
    const reply = nextMessage(peer);
    await back.send(new Uint8Array([4, 2]));
    assert.deepEqual([...(await reply)], [4, 2]);
    await sleep(300);
    assert.deepEqual(page.peers.map((p) => p.id), [bot1.id]);
    assert.deepEqual(bot1.peers.map((p) => p.id), [page.id]);
    assert.equal(bot2.peers.length, 0);
  } finally {
    leaveAll([page, bot1, bot2]);
  }
});

test('manual : connect() is idempotent — concurrent and later calls give the same peer, one offer', async () => {
  const log = [];
  const [a, b] = await enter(fresh(), 2, (() => { let i = 0; return (o) => (i++ === 0 ? joinWith(tapped(log)) : nodeJoin)(o); })());
  try {
    const [p1, p2] = await Promise.all([a.connect(b.id), a.connect(b.id)]);
    assert.equal(p1, p2);
    assert.equal(await a.connect(b.id), p1);
    assert.equal(offersIn(log), 1);
  } finally {
    leaveAll([a, b]);
  }
});

test('manual : crossing connects (both sides at once) make one link, the smaller id wins', async () => {
  const logs = { a: [], b: [] };
  const options = fresh();
  const a = joinWith(tapped(logs.a))(options);
  await a.ready;
  const b = joinWith(tapped(logs.b))(options);
  await b.ready;
  await waitFor(() => a.members.length === 1 && b.members.length === 1);
  try {
    const [pa, pb] = await Promise.all([a.connect(b.id), b.connect(a.id)]);
    assert.equal(offersIn(logs.a), 1, 'a offered');
    assert.equal(offersIn(logs.b), 1, 'b offered : the offers did cross');
    assert.equal(pa.id, b.id);
    assert.equal(pb.id, a.id);
    assert.equal(pa.via, 'direct', 'the winning offer went through, not the relay fallback');
    assert.equal(pb.via, 'direct');
    await sleep(300);
    assert.equal(a.peers.length, 1);
    assert.equal(b.peers.length, 1);
    const got = nextMessage(pb);
    await pa.send('un seul lien');
    assert.equal(await got, 'un seul lien');
  } finally {
    leaveAll([a, b]);
  }
});

test('manual : members follows arrivals and departures ; connect to an absent id, or after leave(), rejects closed', async () => {
  const options = fresh();
  const a = nodeJoin(options);
  await a.ready;
  const seen = collect(a, 'members');
  try {
    assert.deepEqual(a.members, []);
    await assert.rejects(a.connect('nobody'), (e) => e.code === 'closed' && e.fatal === false);
    const b = nodeJoin(options);
    await b.ready;
    await waitFor(() => seen.length === 1);
    assert.deepEqual(seen[0], [b.id]);
    b.leave();
    await waitFor(() => seen.length === 2);
    assert.deepEqual(seen[1], []);
    assert.deepEqual(a.members, []);
  } finally {
    a.leave();
  }
  await assert.rejects(a.connect('nobody'), (e) => e.code === 'closed');
});

test('manual : a pending connect() rejects closed when the room is left', async () => {
  const [a, b] = await enter(fresh({ connectMs: 5000 }), 2, (() => { let i = 0; return (o) => (i++ === 0 ? joinWith(brokenIce()) : nodeJoin)(o); })());
  try {
    const pending = a.connect(b.id);
    await sleep(100);
    a.leave();
    await assert.rejects(pending, (e) => e.code === 'closed');
  } finally {
    leaveAll([a, b]);
  }
});

test('manual : no direct path → connect() resolves with a relay peer', async () => {
  const [a, b] = await enter(fresh({ connectMs: 300 }), 2, joinWith(brokenIce()));
  try {
    const arrived = once(b, 'peer', 8000);
    const peer = await a.connect(b.id);
    assert.equal(peer.via, 'relay');
    const [back] = await arrived;
    assert.equal(back.via, 'relay');
    const got = nextMessage(back);
    await peer.send('par le relay');
    assert.equal(await got, 'par le relay');
  } finally {
    leaveAll([a, b]);
  }
});

test('manual : no direct path and fallback: false → connect() rejects ice', async () => {
  const [a, b] = await enter(fresh({ connectMs: 300, fallback: false }), 2, joinWith(brokenIce()));
  try {
    await assert.rejects(a.connect(b.id), (e) => e.code === 'ice' && e.fatal === false);
    assert.equal(a.peers.length, 0);
  } finally {
    leaveAll([a, b]);
  }
});

test('mixed : an automatic newcomer still links to a manual member ; manual members stay apart', async () => {
  const options = fresh();
  const [m1, m2] = await enter(options, 2);
  const auto = nodeJoin({ ...options, connect: 'auto' });
  try {
    await auto.ready;
    await waitFor(() => auto.peers.length === 2, 10_000, 'the newcomer linked to both');
    assert.deepEqual(m1.peers.map((p) => p.id), [auto.id]);
    assert.deepEqual(m2.peers.map((p) => p.id), [auto.id]);
    assert.ok(await silent(m1, 'peer', 300));
  } finally {
    leaveAll([m1, m2, auto]);
  }
});

test('auto (the default) : connect() on a linked peer returns it, without a new offer', async () => {
  const options = fresh({ connect: undefined });
  const log = [];
  const a = nodeJoin(options);
  await a.ready;
  const b = joinWith(tapped(log))(options);
  await b.ready;
  try {
    await waitFor(() => a.peers.length === 1 && b.peers.length === 1);
    const before = offersIn(log);
    assert.equal(await b.connect(a.id), b.peers[0]);
    assert.equal(offersIn(log), before);
  } finally {
    leaveAll([a, b]);
  }
});
