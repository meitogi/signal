// The client and its relay : pings, idle closes, restarts, bans, and the sealed relay fallback.
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { newSecret } from '../src/crypto.js';
import { brokenIce, browserShaped, noPing, collect, joinWith, leaveAll, meet, nextMessage, nodeJoin, once, silent, sleep, startRelay, tapped, urlOf, waitFor } from './client-helpers.js';

const fresh = (url, extra) => ({ relay: url, room: 'salon', secret: newSecret(), iceServers: [], ...extra });
const hellos = (frames) => frames.filter((f) => f.dir === 'out' && f.text.startsWith('{"type":"hello"')).length;

test('pings every 5 s keep the socket alive past a 6 s idle limit', { timeout: 20_000 }, async () => {
  const relay = await startRelay({ idleMs: 6000 });
  const frames = [];
  const room = joinWith(tapped(frames))(fresh(urlOf(relay)));
  await room.ready;
  try {
    const errors = collect(room, 'error');
    await sleep(8000);
    assert.deepEqual(errors, []);
    assert.equal(hellos(frames), 1, 'never reconnected');
    const pings = frames.filter((f) => f.dir === 'out' && f.text.startsWith('{"type":"ping"')).length;
    assert.ok(pings >= 1, 'pinged');
    assert.ok(frames.some((f) => f.dir === 'in' && f.text.startsWith('{"type":"pong"')));
  } finally {
    room.leave();
    await relay.close();
  }
});

test('4408 : the room reconnects with backoff, the direct link survives, no re-offer', { timeout: 20_000 }, async () => {
  const relay = await startRelay({ idleMs: 400 });
  const frames = [];
  const options = fresh(urlOf(relay));
  const [a, b] = await meet(options, 2, joinWith(tapped(frames)));
  try {
    const errorsA = collect(a, 'error');
    await waitFor(() => hellos(frames) >= 4, 8000, 'two reconnects');
    assert.ok(errorsA.some((e) => e.code === 'relay' && e.fatal === false && /idle/.test(e.message)), `errors : ${errorsA.map((e) => e.message)}`);
    assert.equal(a.peers.length, 1);
    assert.equal(b.peers.length, 1);
    const got = nextMessage(b.peers[0]);
    await a.peers[0].send('still here');
    assert.equal(await got, 'still here');
    const offers = frames.filter((f) => f.dir === 'out' && f.text.includes('"kind":"offer"')).length;
    assert.equal(offers, 1, 'one offer in the whole life of the pair');
  } finally {
    leaveAll([a, b]);
    await relay.close();
  }
});

test('relay restart : the rooms come back, a newcomer meets both, the old direct link is intact', { timeout: 30_000 }, async () => {
  let relay = await startRelay();
  const port = relay.port;
  const options = fresh(urlOf(relay));
  const [a, b] = await meet(options);
  try {
    const errors = collect(a, 'error');
    await relay.close();
    await waitFor(() => errors.length >= 1, 5000, 'the room noticed');
    assert.equal(errors[0].code, 'relay');
    assert.equal(errors[0].fatal, false);
    const got = nextMessage(b.peers[0]);
    await a.peers[0].send('relay down, still talking');
    assert.equal(await got, 'relay down, still talking');
    await sleep(1200);
    relay = await startRelay({ port });
    const c = nodeJoin(options);
    await c.ready;
    await waitFor(() => c.peers.length === 2 && a.peers.length === 2 && b.peers.length === 2, 15_000, 'c meets both');
    assert.equal(await silent(a, 'close', 200), true);
    c.leave();
  } finally {
    leaveAll([a, b]);
    await relay.close();
  }
});

test('fallback : no direct path → both get a relay peer, messages flow sealed through the relay', { timeout: 30_000 }, async () => {
  const relay = await startRelay();
  const frames = [];
  const options = fresh(urlOf(relay), { connectMs: 1500 });
  const a = joinWith(brokenIce())(options);
  const b = joinWith(tapped(frames, brokenIce()))(options);
  await Promise.all([a.ready, b.ready]);
  try {
    await waitFor(() => a.peers.length === 1 && b.peers.length === 1, 15_000, 'relay peers');
    assert.equal(a.peers[0].via, 'relay');
    assert.equal(b.peers[0].via, 'relay');
    const got = nextMessage(b.peers[0]);
    await a.peers[0].send('par le relay');
    assert.equal(await got, 'par le relay');
    const back = nextMessage(a.peers[0]);
    await b.peers[0].send(new Uint8Array([4, 5, 6]));
    assert.deepEqual([...(await back)], [4, 5, 6]);
    const big = new Uint8Array(100 * 1024).map((_, i) => i & 255);
    const gotBig = nextMessage(b.peers[0], 15_000);
    await a.peers[0].send(big);
    const received = await gotBig;
    assert.equal(received.length, big.length);
    assert.ok(received.every((v, i) => v === (i & 255)));
    const text = frames.map((f) => f.text).join('\n');
    assert.ok(!text.includes('par le relay'), 'plaintext never on the wire');
    const datas = frames.filter((f) => f.text.startsWith('{"type":"data"'));
    assert.ok(datas.length >= 10, `data frames : ${datas.length}`);
    assert.ok(datas.every((f) => Buffer.byteLength(f.text) <= 16_384), `largest frame ${Math.max(...datas.map((f) => Buffer.byteLength(f.text)))}`);
    const leftA = once(a, 'leave');
    b.peers[0].close();
    assert.equal((await leftA)[1], 'closed');
  } finally {
    leaveAll([a, b]);
    await relay.close();
  }
});

test('fallback: false → error ice, no peer', { timeout: 20_000 }, async () => {
  const relay = await startRelay();
  const options = fresh(urlOf(relay), { connectMs: 1000, fallback: false });
  const a = joinWith(brokenIce())(options);
  const b = joinWith(brokenIce())(options);
  await Promise.all([a.ready, b.ready]);
  try {
    const [error] = await once(b, 'error', 8000);
    assert.equal(error.code, 'ice');
    assert.equal(error.fatal, false);
    await sleep(300);
    assert.equal(a.peers.length, 0);
    assert.equal(b.peers.length, 0);
    assert.equal(await silent(a, 'close', 200), true);
  } finally {
    leaveAll([a, b]);
    await relay.close();
  }
});

test('relay unreachable : error relay, ready pending, leave() closes with null', async () => {
  const room = nodeJoin(fresh('ws://127.0.0.1:1'));
  const [error] = await once(room, 'error', 5000);
  assert.equal(error.code, 'relay');
  assert.equal(error.fatal, false);
  let settled = false;
  room.ready.then(() => { settled = true; }, () => { settled = true; });
  await sleep(100);
  assert.equal(settled, false);
  const closed = once(room, 'close');
  room.leave();
  assert.equal((await closed)[0], null);
  await assert.rejects(room.ready, (e) => e.code === 'closed');
});

test('HTTP 429 at upgrade : banned in Node before entry ; a browser-shaped socket sees a plain relay error', async () => {
  const relay = await startRelay({ connMax: 0 });
  try {
    const node = nodeJoin(fresh(urlOf(relay)));
    const closed = once(node, 'close');
    await assert.rejects(node.ready, (e) => e.code === 'banned' && e.fatal === true);
    assert.equal((await closed)[0].code, 'banned');
    const blind = joinWith(browserShaped())(fresh(urlOf(relay)));
    const [error] = await once(blind, 'error', 5000);
    assert.equal(error.code, 'relay');
    assert.equal(error.fatal, false);
    blind.leave();
  } finally {
    await relay.close();
  }
});

test('a relay-via peer is dropped when it leaves the relay, and re-linked when it comes back', { timeout: 40_000 }, async () => {
  const relay = await startRelay({ idleMs: 6000 });
  const options = fresh(urlOf(relay), { connectMs: 1200 });
  // b never pings : the relay cuts it (4408) 6 s after each hello ; a pings and stays.
  const a = joinWith(brokenIce())(options);
  const b = joinWith(noPing(brokenIce()))(options);
  await Promise.all([a.ready, b.ready]);
  try {
    await waitFor(() => a.peers.length === 1 && b.peers.length === 1, 15_000, 'relay peers');
    assert.equal(a.peers[0].via, 'relay');
    const leaves = collect(a, 'leave');
    const errorsB = collect(b, 'error');
    await waitFor(() => leaves.length >= 1, 10_000, 'a lost its relay peer when b was cut');
    assert.equal(leaves[0][1], 'relay');
    assert.ok(errorsB.some((e) => e.code === 'relay' && /idle/.test(e.message)), `b saw the 4408 : ${errorsB.map((e) => e.message)}`);
    await waitFor(() => a.peers.length === 1 && b.peers.length === 1, 20_000, 'linked again after the reconnect');
    assert.equal(b.peers[0].via, 'relay');
    const got = nextMessage(a.peers[0]);
    await b.peers[0].send('de retour');
    assert.equal(await got, 'de retour');
  } finally {
    leaveAll([a, b]);
    await relay.close();
  }
});
