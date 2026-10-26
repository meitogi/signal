// Test-only stand-in for `werift` as seen by src/node-platform.js, put there by bench.mjs's resolve
// hook : the same module, with every RTCPeerConnection recorded so the bench can call werift's own
// getStats() (NodePeerConnection keeps its werift connection private). Nothing else changes — no port
// range, no extra host address : the candidates gathered are the ones the library gathers.
export * from 'werift';
import { RTCPeerConnection as Real } from 'werift';

/** Every connection built, oldest first. */
export const built = [];

export class RTCPeerConnection extends Real {
  constructor(config) {
    super(config);
    built.push(this);
  }
}
