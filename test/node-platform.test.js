// Node's platform against the contract : `iceServers: []` asks no STUN server (werift falls back to
// its built-in one when given none).
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { NodePeerConnection } from '../src/node-platform.js';

/** Every candidate a fresh offerer gathers, until the end-of-candidates null (or `ms`). */
async function gather(iceServers, ms = 8000) {
  const pc = new NodePeerConnection({ iceServers });
  const candidates = [];
  const done = new Promise((resolve) => {
    pc.addEventListener('icecandidate', ({ candidate }) => {
      if (candidate === null) resolve();
      else candidates.push(candidate.candidate);
    });
    setTimeout(resolve, ms);
  });
  pc.createDataChannel('signal');
  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await done;
  pc.close();
  return candidates;
}

test('iceServers: [] gathers host candidates only : no srflx, no STUN request', async () => {
  const candidates = await gather([]);
  assert.ok(candidates.some((c) => / typ host/.test(c)), `host candidates : ${candidates}`);
  assert.deepEqual(candidates.filter((c) => / typ srflx/.test(c)), []);
});
