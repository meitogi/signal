// A Peer after its proof : the object the app talks to, the same over a DataChannel (`direct`)
// and through the relay (`relay`). It frames messages (frames.js), serialises sends so fragments
// never interleave, waits on backpressure, and caps what the other side may send us.
import { Emitter } from './emitter.js';
import { SignalError } from './errors.js';
import { createAssembler, encodeFrames, toBytes } from './frames.js';

/** Buffered bytes above which a direct send waits, and the low-water mark that resumes it. */
export const BP_HIGH = 1024 * 1024;
export const BP_LOW = 256 * 1024;
/** Receive cap per peer, in frames per second (sustained / at once). Tunable with `limits`. */
export const PEER_RATE = 1000;
export const PEER_BURST = 2000;

/**
 * @param {object} o
 * @param {string} o.id
 * @param {'direct'|'relay'} o.via
 * @param {number} o.maxMessageBytes
 * @param {{peerRate?: number, peerBurst?: number}} [o.limits]
 * @param {number} [o.chunk] - frame payload size (relay frames are smaller)
 * @param {(frame: Uint8Array) => void|Promise<void>} o.sendFrame - resolves once the frame is accepted
 * @param {() => void} o.shutdown - closes the transport (idempotent)
 * @returns {Emitter & {id, via, closed, send, close, receive(frame): void, end(reason): void}}
 *   `receive` is fed every frame from the other side ; `end` closes from inside (transport gone).
 */
export function createPeer({ id, via, maxMessageBytes, limits, chunk, sendFrame, shutdown }) {
  const peer = new Emitter();
  const rate = limits?.peerRate ?? PEER_RATE;
  const burst = limits?.peerBurst ?? PEER_BURST;
  let tokens = burst;
  let stamp = Date.now();
  let outbox = Promise.resolve();
  const assemble = createAssembler(maxMessageBytes, (data) => peer.emit('message', data));

  peer.id = id;
  peer.via = via;
  peer.closed = false;

  peer.end = (reason) => {
    if (peer.closed) return;
    peer.closed = true;
    shutdown();
    peer.emit('close', reason);
  };

  peer.receive = (frame) => {
    const now = Date.now();
    tokens = Math.min(burst, tokens + ((now - stamp) * rate) / 1000);
    stamp = now;
    if (tokens < 1) return peer.end('peer-rate');
    tokens--;
    const problem = assemble(frame);
    if (problem !== null) peer.end(problem);
  };

  peer.send = (data) => {
    if (peer.closed) return Promise.reject(new SignalError('closed', `peer ${id} is closed`));
    const conv = toBytes(data);
    if (conv === null) return Promise.reject(new SignalError('options', 'send() takes a string, a Uint8Array or an ArrayBuffer'));
    const bytes = conv.bytes;
    if (bytes.length > maxMessageBytes) return Promise.reject(new SignalError('too-big', `${bytes.length} bytes > maxMessageBytes ${maxMessageBytes}`));
    const frames = encodeFrames(bytes, conv.text, chunk);
    const next = outbox.then(async () => {
      for (const frame of frames) {
        if (peer.closed) throw new SignalError('closed', `peer ${id} closed while sending`);
        await sendFrame(frame);
      }
    });
    outbox = next.catch(() => {});
    return next;
  };

  peer.close = () => peer.end('closed');

  return peer;
}

/** A direct peer on an open DataChannel. */
export function directPeer({ id, pc, channel, maxMessageBytes, limits }) {
  channel.bufferedAmountLowThreshold = BP_LOW;
  const drained = () => new Promise((resolve) => {
    if (channel.bufferedAmount <= BP_LOW || channel.readyState !== 'open') return resolve();
    const done = () => {
      channel.removeEventListener('bufferedamountlow', done);
      channel.removeEventListener('close', done);
      resolve();
    };
    channel.addEventListener('bufferedamountlow', done);
    channel.addEventListener('close', done);
  });
  const peer = createPeer({
    id,
    via: 'direct',
    maxMessageBytes,
    limits,
    sendFrame: async (frame) => {
      if (channel.readyState !== 'open') throw new SignalError('closed', `peer ${id} is closed`);
      channel.send(frame);
      if (channel.bufferedAmount > BP_HIGH) await drained();
    },
    shutdown: () => { try { channel.close(); } catch { /* already */ } pc.close(); },
  });
  // After the proof the channel carries binary frames only : text here is a protocol violation.
  channel.addEventListener('message', ({ data }) => {
    if (typeof data === 'string') return peer.end('malformed');
    peer.receive(new Uint8Array(data));
  });
  channel.addEventListener('close', () => peer.end('closed'));
  pc.addEventListener('connectionstatechange', () => {
    const state = pc.connectionState;
    if (state === 'failed' || state === 'closed' || state === 'disconnected') peer.end('closed');
  });
  return peer;
}
