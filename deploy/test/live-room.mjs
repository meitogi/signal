// Live check of a deployed relay : two @meitogi/signal/node peers join the same room through it,
// exchange one message each way, and leave. From the repository root, after `npm ci` :
//   node deploy/test/live-room.mjs wss://relay.example.org
// Prints each step ; exit 0 = PASS. Fails within 30 s, saying which step it was waiting for.
import { joinRoom, newSecret } from '../../src/node.js';

const relay = process.argv[2];
if (!relay) {
  console.error('usage : node deploy/test/live-room.mjs <wss://relay>');
  process.exit(2);
}
let step = 'joining';
const guard = setTimeout(() => { console.error(`FAIL timed out while ${step}`); process.exit(1); }, 30_000);
const say = (line) => console.log(`[live-room] ${line}`);

const options = { relay, room: 'live-check', secret: newSecret() };
const a = joinRoom(options);
const b = joinRoom(options);
for (const r of [a, b]) r.on('error', (e) => { console.error(`FAIL ${e.code} : ${e.message}`); process.exit(1); });
await Promise.all([a.ready, b.ready]);
say(`both in the room (ids ${a.id.slice(0, 6)}, ${b.id.slice(0, 6)})`);

step = 'waiting for the two peers to connect';
const peerOf = (room) => new Promise((resolve) => (room.peers.length ? resolve(room.peers[0]) : room.on('peer', resolve)));
const [pa, pb] = await Promise.all([peerOf(a), peerOf(b)]);
say(`connected, path : ${pa.via}`);

step = 'exchanging messages';
const got = (peer) => new Promise((resolve) => peer.on('message', resolve));
const atB = got(pb);
await pa.send('ping from a');
const atA = got(pa);
await pb.send('pong from b');
const [m1, m2] = await Promise.all([atB, atA]);
if (m1 !== 'ping from a' || m2 !== 'pong from b') {
  console.error(`FAIL wrong payloads : ${JSON.stringify([m1, m2])}`);
  process.exit(1);
}
say('messages arrived both ways');
a.leave();
b.leave();
clearTimeout(guard);
say('PASS');
process.exit(0);
