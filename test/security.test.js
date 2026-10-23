// What a member who knows the secret — or the relay itself — can and cannot do to a peer.
import { strict as assert } from 'node:assert';
import { after, before, test } from 'node:test';
import { newSecret } from '../src/crypto.js';
import { attacker, collect, joinWith, leaveAll, meet, nodeJoin, once, silent, sleep, startRelay, tapped, urlOf, waitFor } from './client-helpers.js';

let relay;
let url;
before(async () => { relay = await startRelay(); url = urlOf(relay); });
after(() => relay.close());

const fresh = (extra) => ({ relay: url, room: 'salon', secret: newSecret(), ...extra });
const blocks = (frames) => frames.filter((f) => f.dir === 'out' && f.text.startsWith('{"type":"block"')).map((f) => JSON.parse(f.text).id);

test('the wire carries no SDP, candidate, fingerprint or room name : only sealed boxes', async () => {
  const frames = [];
  const options = fresh();
  const rooms = await meet(options, 2, joinWith(tapped(frames)));
  try {
    const text = frames.map((f) => f.text).join('\n');
    for (const leak of ['a=fingerprint', 'candidate:', 'v=0', 'sdp', 'salon', options.secret, 'proof']) {
      assert.ok(!text.includes(leak), `${leak} seen on the wire`);
    }
    const signals = frames.filter((f) => f.text.startsWith('{"type":"signal"')).map((f) => JSON.parse(f.text));
    assert.ok(signals.length >= 4, `signals : ${signals.length}`);
    for (const s of signals) {
      assert.deepEqual(Object.keys(s).sort(), s.from ? ['box', 'from', 'kind', 'type'] : ['box', 'kind', 'to', 'type']);
      assert.match(s.box, /^[A-Za-z0-9_-]+$/);
      assert.ok(['offer', 'answer', 'ice'].includes(s.kind));
    }
  } finally {
    leaveAll(rooms);
  }
});

test('a tampered box is dropped and its sender blocked on the wire', async () => {
  const frames = [];
  const options = fresh();
  const victim = joinWith(tapped(frames))({ ...options, iceServers: [] });
  await victim.ready;
  const evil = await attacker(url, options);
  try {
    const errors = collect(victim, 'error');
    await evil.signal(victim.id, 'offer', { kind: 'offer', sdp: 'v=0', nonce: 'n' }, { tamper: true });
    await waitFor(() => blocks(frames).includes(evil.id), 3000, 'block on the wire');
    assert.ok(errors.some((e) => e.code === 'proof' && e.fatal === false), 'error proof emitted');
    assert.equal(victim.peers.length, 0);
    // Blocked : a valid offer from the same id no longer reaches the victim.
    const o = await evil.offer(victim.id);
    assert.equal(await evil.ws.silent('signal', 800), true, 'no answer after the block');
    o.pc.close();
  } finally {
    await evil.close();
    victim.leave();
  }
});

test('a replayed box is dropped and its sender blocked', async () => {
  const frames = [];
  const options = fresh();
  const victim = joinWith(tapped(frames))({ ...options, iceServers: [], connectMs: 1500 });
  await victim.ready;
  const evil = await attacker(url, options);
  try {
    const o = await evil.offer(victim.id);
    const answer = await evil.nextSignal();
    assert.equal(answer.kind, 'answer');
    assert.equal(answer.inner.kind, 'answer');
    assert.deepEqual(blocks(frames), []);
    evil.sendBox(victim.id, 'offer', o.box); // the very same box again
    await waitFor(() => blocks(frames).includes(evil.id), 3000, 'block after replay');
  } finally {
    await evil.close();
    victim.leave();
  }
});

test('a box whose sealed kind differs from the cleartext kind is dropped and its sender blocked', async () => {
  const frames = [];
  const options = fresh();
  const victim = joinWith(tapped(frames))({ ...options, iceServers: [] });
  await victim.ready;
  const evil = await attacker(url, options);
  try {
    // Says `ice` to the relay (not counted against the offer cap), carries an offer inside.
    await evil.signal(victim.id, 'ice', { kind: 'offer', sdp: 'v=0', nonce: 'n' });
    await waitFor(() => blocks(frames).includes(evil.id), 3000, 'block on kind mismatch');
    assert.equal(await evil.ws.silent('signal', 500), true, 'nothing answered');
  } finally {
    await evil.close();
    victim.leave();
  }
});

test('before the proof : a message over 1 KB, or not the proof, cuts the peer unread ; no peer event', async () => {
  const options = fresh();
  for (const junk of ['x'.repeat(1025), new Uint8Array(10), JSON.stringify({ t: 'ok' }), 'not json', JSON.stringify({ t: 'proof', proof: 'AAAA' })]) {
    const victim = nodeJoin({ ...options, iceServers: [], connectMs: 3000 });
    await victim.ready;
    const evil = await attacker(url, options);
    try {
      const errors = collect(victim, 'error');
      const o = await evil.offer(victim.id);
      await o.complete();
      assert.equal(o.channel.readyState, 'open');
      const closed = new Promise((r) => o.channel.addEventListener('close', r, { once: true }));
      o.channel.send(junk);
      await closed;
      assert.ok(errors.some((e) => e.code === 'proof'), `error proof for ${typeof junk === 'string' ? junk.slice(0, 20) : 'binary'}`);
      assert.equal(victim.peers.length, 0);
    } finally {
      await evil.close();
      victim.leave();
    }
  }
});

test('a real proof over the channel yields ok and a peer', async () => {
  const options = fresh();
  const victim = nodeJoin({ ...options, iceServers: [] });
  await victim.ready;
  const evil = await attacker(url, options);
  try {
    const { mac } = await import('../src/crypto.js');
    const { transcript } = await import('../src/sdp.js');
    const o = await evil.offer(victim.id);
    await o.complete();
    const t = { offerSdp: o.sdp, answerSdp: o.answer.sdp, roomId: evil.roomId, offerNonce: o.nonce, answerNonce: o.answer.nonce };
    const { verifyMac } = await import('../src/crypto.js');
    assert.equal(await verifyMac(evil.auth, transcript('answerer', t), o.answer.proof), true, 'the victim proved the secret in its answer');
    const reply = new Promise((r) => o.channel.addEventListener('message', (e) => r(e.data), { once: true }));
    const peer = once(victim, 'peer');
    o.channel.send(JSON.stringify({ t: 'proof', proof: await mac(evil.auth, transcript('offerer', t)) }));
    assert.deepEqual(JSON.parse(await reply), { t: 'ok' });
    assert.equal((await peer)[0].id, evil.id);
  } finally {
    await evil.close();
    victim.leave();
  }
});

test('at most 4 inbound handshakes : the 5th concurrent offer is dropped ; a duplicate offer is dropped', async () => {
  const frames = [];
  const options = fresh();
  const victim = joinWith(tapped(frames))({ ...options, iceServers: [], connectMs: 6000 });
  await victim.ready;
  const evils = [];
  for (let i = 0; i < 5; i++) evils.push(await attacker(url, options));
  try {
    const errors = collect(victim, 'error');
    const offers = await Promise.all(evils.map((e) => e.offer(victim.id)));
    // The victim's answer comes before its candidates on each socket (one FIFO queue).
    const answered = await Promise.all(evils.map((e) => e.ws.next('signal', 1000).then((m) => m.kind === 'answer', () => false)));
    assert.equal(answered.filter(Boolean).length, 4, `answers : ${answered}`);
    assert.ok(errors.some((e) => e.code === 'timeout' && /in flight/.test(e.message)));
    // A second offer from one of the four, during its handshake : no second answer, no block.
    const again = evils.find((e, i) => answered[i]);
    await again.offer(victim.id);
    await sleep(600);
    const kinds = [];
    for (;;) { const m = await again.ws.next('signal', 50).catch(() => null); if (m === null) break; kinds.push(m.kind); }
    assert.ok(!kinds.includes('answer'), `duplicate offer not answered (got ${kinds})`);
    assert.deepEqual(blocks(frames), []);
    for (const o of offers) o.pc.close();
  } finally {
    for (const e of evils) await e.close();
    victim.leave();
  }
});

test('peer-rate : a peer flooding us after the proof is cut, the room goes on', async () => {
  const options = fresh();
  const victim = nodeJoin({ ...options, iceServers: [], limits: { peerRate: 50, peerBurst: 50 } });
  const flooder = nodeJoin({ ...options, iceServers: [] });
  await Promise.all([victim.ready, flooder.ready]);
  await waitFor(() => victim.peers.length === 1 && flooder.peers.length === 1);
  try {
    const errors = collect(victim, 'error');
    const left = once(victim, 'leave');
    const p = flooder.peers[0];
    for (let i = 0; i < 200; i++) p.send('x').catch(() => {});
    const [peer, reason] = await left;
    assert.equal(peer.id, flooder.id);
    assert.equal(reason, 'peer-rate');
    assert.ok(errors.some((e) => e.code === 'peer-rate' && e.fatal === false));
    await waitFor(() => flooder.peers.length === 0, 5000, 'flooder sees the cut');
    assert.equal(await silent(victim, 'close', 300), true, 'the room is not closed');
  } finally {
    leaveAll([victim, flooder]);
  }
});
