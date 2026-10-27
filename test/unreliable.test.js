// The unreliable channel : `peer.send(data, { reliable: false })` over a pre-negotiated, unordered,
// no-retransmit DataChannel ; one frame per message, never waits, shares the receive cap, takes the
// reliable path over the relay, and reads nothing before the proof.
import { strict as assert } from 'node:assert';
import { after, before, test } from 'node:test';
import { mac, newSecret } from '../src/crypto.js';
import { CHUNK } from '../src/frames.js';
import { UNRELIABLE } from '../src/room.js';
import { transcript } from '../src/sdp.js';
import { attacker, brokenIce, collect, joinWith, leaveAll, meet, nextMessage, nodeJoin, once, recorded, silent, sleep, startRelay, urlOf, waitFor } from './client-helpers.js';

let relay;
let url;
before(async () => { relay = await startRelay(); url = urlOf(relay); });
after(() => relay.close());

const fresh = (extra) => ({ relay: url, room: 'salon', secret: newSecret(), iceServers: [], ...extra });

test('direct : both sides hold a negotiated, unordered, no-retransmit channel ; strings and bytes cross it, both ways', async () => {
  const seen = [];
  const options = fresh();
  const a = joinWith(recorded(seen))(options);
  const b = joinWith(recorded(seen))(options);
  await Promise.all([a.ready, b.ready]);
  await waitFor(() => a.peers.length === 1 && b.peers.length === 1);
  try {
    const lossy = seen.filter((s) => s.label === UNRELIABLE.label);
    assert.equal(lossy.length, 2, 'one per side, both created locally (negotiated : no datachannel event)');
    for (const { how, channel } of lossy) {
      assert.equal(how, 'created');
      assert.deepEqual(
        [channel.negotiated, channel.id, channel.ordered, channel.maxRetransmits, channel.readyState],
        [true, UNRELIABLE.options.id, false, 0, 'open'],
      );
    }
    const got = nextMessage(b.peers[0]);
    assert.equal(await a.peers[0].send('vite', { reliable: false }), true);
    assert.equal(await got, 'vite');
    const back = nextMessage(a.peers[0]);
    assert.equal(await b.peers[0].send(new Uint8Array([9, 8, 7]), { reliable: false }), true);
    assert.deepEqual([...(await back)], [9, 8, 7]);
  } finally {
    leaveAll([a, b]);
  }
});

test('size : one frame at most (CHUNK bytes) ; above it, or above maxMessageBytes, too-big', async () => {
  const [a, b] = await meet(fresh());
  try {
    const full = new Uint8Array(CHUNK).map((_, i) => i & 255);
    const got = nextMessage(b.peers[0]);
    assert.equal(await a.peers[0].send(full, { reliable: false }), true);
    const received = await got;
    assert.equal(received.length, CHUNK);
    assert.ok(received.every((v, i) => v === (i & 255)));
    await assert.rejects(a.peers[0].send(new Uint8Array(CHUNK + 1), { reliable: false }), (e) => e.code === 'too-big' && e.fatal === false);
    await assert.rejects(a.peers[0].send('é'.repeat(CHUNK / 2 + 1), { reliable: false }), (e) => e.code === 'too-big', 'counted in UTF-8 bytes');
  } finally {
    leaveAll([a, b]);
  }
  const [c, d] = await meet(fresh({ maxMessageBytes: 1000 }));
  try {
    await assert.rejects(c.peers[0].send(new Uint8Array(1001), { reliable: false }), (e) => e.code === 'too-big');
  } finally {
    leaveAll([c, d]);
  }
});

test('never waits : with the channel backed up, sends resolve false at once instead of queueing', async () => {
  const [a, b] = await meet(fresh());
  try {
    const received = [];
    b.peers[0].on('message', (m) => received.push(m));
    const frame = new Uint8Array(CHUNK);
    const started = Date.now();
    const results = [];
    for (let i = 0; i < 400; i++) results.push(a.peers[0].send(frame, { reliable: false }));
    const outcomes = await Promise.all(results);
    const elapsed = Date.now() - started;
    const sent = outcomes.filter((x) => x === true).length;
    assert.ok(outcomes.every((x) => x === true || x === false), 'each send resolves true or false');
    assert.ok(sent < 400 && sent >= 4, `${sent} of 400 sent : 6.4 MB at once must overflow the 64 KB mark`);
    assert.ok(elapsed < 1000, `400 sends resolved in ${elapsed} ms : no waiting`);
    await sleep(800);
    assert.ok(received.length <= sent, `received ${received.length} > sent ${sent}`);
    assert.equal(await a.peers[0].send('later', { reliable: false }), true, 'drained : sends go again');
    await waitFor(() => received.at(-1) === 'later', 3000, 'the message sent once drained');
  } finally {
    leaveAll([a, b]);
  }
});

test('one receive cap per peer counts both channels : 30 reliable + 30 unreliable over a 50-frame burst is cut', async () => {
  const options = fresh();
  const victim = nodeJoin({ ...options, limits: { peerRate: 1, peerBurst: 50 } });
  const flooder = nodeJoin(options);
  await Promise.all([victim.ready, flooder.ready]);
  await waitFor(() => victim.peers.length === 1 && flooder.peers.length === 1);
  try {
    const errors = collect(victim, 'error');
    const left = once(victim, 'leave');
    const p = flooder.peers[0];
    for (let i = 0; i < 30; i++) p.send(`r${i}`).catch(() => {});
    const lossy = [];
    for (let i = 0; i < 30; i++) lossy.push(p.send(`u${i}`, { reliable: false }).catch(() => null));
    // Each half stays under the 50-frame burst : only a cap shared by both channels cuts.
    assert.equal((await Promise.all(lossy)).filter((x) => x === true).length, 30, 'the 30 went on the unreliable channel');
    const [peer, reason] = await left;
    assert.equal(peer.id, flooder.id);
    assert.equal(reason, 'peer-rate');
    assert.ok(errors.some((e) => e.code === 'peer-rate'));
  } finally {
    leaveAll([victim, flooder]);
  }
});

test('over the relay : reliable: false takes the reliable path, same size rule', { timeout: 30_000 }, async () => {
  const options = fresh({ connectMs: 1 });
  const b = joinWith(brokenIce())(options);
  await b.ready;
  const a = joinWith(brokenIce())(options);
  await a.ready;
  try {
    await waitFor(() => a.peers.length === 1 && b.peers.length === 1, 15_000, 'relay peers');
    assert.equal(a.peers[0].via, 'relay');
    const got = [];
    b.peers[0].on('message', (m) => got.push(m));
    for (let i = 0; i < 20; i++) assert.equal(await a.peers[0].send(`r${i}`, { reliable: false }), true);
    await waitFor(() => got.length === 20, 5000, '20 relayed messages');
    assert.deepEqual(got, Array.from({ length: 20 }, (_, i) => `r${i}`), 'all of them, in order');
    await assert.rejects(a.peers[0].send(new Uint8Array(CHUNK + 1), { reliable: false }), (e) => e.code === 'too-big');
  } finally {
    leaveAll([a, b]);
  }
});

/** A hand-driven offerer with the unreliable channel, up to an open channel (no proof yet). */
async function evilLink(options) {
  const victim = nodeJoin({ ...options, connectMs: 5000 });
  await victim.ready;
  const evil = await attacker(url, options);
  const o = await evil.offer(victim.id, { unreliable: true });
  await o.complete();
  await waitFor(() => o.unreliable.readyState === 'open', 3000, 'the unreliable channel open');
  const prove = async () => {
    const t = { offerSdp: o.sdp, answerSdp: o.answer.sdp, roomId: evil.roomId, offerNonce: o.nonce, answerNonce: o.answer.nonce };
    const peer = once(victim, 'peer');
    o.channel.send(JSON.stringify({ t: 'proof', proof: await mac(evil.auth, transcript('offerer', t)) }));
    return (await peer)[0];
  };
  return { victim, evil, o, prove };
}

test('before the proof : what comes on the unreliable channel is discarded unread, the handshake goes on', async () => {
  const { victim, evil, o, prove } = await evilLink(fresh());
  try {
    const errors = collect(victim, 'error');
    // Well-formed frames (flags LAST|TEXT + payload) : read, they would surface as messages.
    for (let i = 0; i < 20; i++) o.unreliable.send(new Uint8Array([3, 0x65, 0x61, 0x72, 0x6c, 0x79]));
    o.unreliable.send('{"t":"proof","proof":"AAAA"}');
    await sleep(300);
    const peer = await prove();
    const messages = collect(peer, 'message');
    await sleep(300);
    assert.deepEqual(messages, [], 'nothing sent before the proof surfaces after it');
    assert.deepEqual(errors, [], 'no cut, no error : the pre-proof frames were never looked at');
    // The same frame after the proof is read : the earlier silence was the gate, not a dead channel.
    o.unreliable.send(new Uint8Array([3, 0x6c, 0x61, 0x74, 0x65]));
    await waitFor(() => messages.length === 1, 3000, 'the post-proof frame');
    assert.equal(messages[0], 'late');
  } finally {
    await evil.close();
    victim.leave();
  }
});

test('any other channel a peer opens is closed ; the peer and its two channels stay', async () => {
  const { victim, evil, o, prove } = await evilLink(fresh());
  try {
    const peer = await prove();
    const extra = o.pc.createDataChannel('extra');
    await new Promise((resolve) => extra.addEventListener('close', resolve, { once: true }));
    assert.equal(victim.peers.length, 1);
    assert.equal(await silent(peer, 'close', 300), true);
    const got = nextMessage(peer);
    o.unreliable.send(new Uint8Array([3, 0x6f, 0x6b]));
    assert.equal(await got, 'ok');
  } finally {
    await evil.close();
    victim.leave();
  }
});
