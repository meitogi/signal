// A Peer after its proof : the object the app talks to, the same over a DataChannel (`direct`)
// and through the relay (`relay`). It frames messages (frames.js), serialises sends so fragments
// never interleave, waits on backpressure, and caps what the other side may send us.
//
// `send(data, { reliable: false })` : one frame, on the direct peer's unreliable channel, never
// waiting — above UNRELIABLE_HIGH buffered it is dropped and resolves false. A relay peer has no such
// channel : the message takes the reliable path. Both channels draw on the same receive cap.
import { Emitter } from './emitter.js';
import { SignalError } from './errors.js';
import { CHUNK, createAssembler, encodeFrames, isWhole, toBytes } from './frames.js';

/** Buffered bytes above which a direct send waits, and the low-water mark that resumes it. */
export const BP_HIGH = 1024 * 1024;
export const BP_LOW = 256 * 1024;
/** Buffered bytes on the unreliable channel above which an unreliable send is dropped. */
export const UNRELIABLE_HIGH = 64 * 1024;
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
 * @param {(frame: Uint8Array) => boolean} [o.sendUnreliable] - false when dropped ; absent = reliable path
 * @param {() => void} o.shutdown - closes the transport (idempotent)
 * @returns {Emitter & {id, via, closed, send, close, receive(frame): void, receiveUnreliable(frame): void, end(reason): void}}
 *   `receive` / `receiveUnreliable` are fed every frame from the other side ; `end` closes from inside (transport gone).
 */
export function createPeer({ id, via, maxMessageBytes, limits, chunk, sendFrame, sendUnreliable, shutdown }) {
  const peer = new Emitter();
  const rate = limits?.peerRate ?? PEER_RATE;
  const burst = limits?.peerBurst ?? PEER_BURST;
  let tokens = burst;
  let stamp = Date.now();
  let outbox = Promise.resolve();
  const deliver = (data) => peer.emit('message', data);
  const assemble = createAssembler(maxMessageBytes, deliver);
  const assembleUnreliable = createAssembler(maxMessageBytes, deliver);
  const unreliableMax = Math.min(CHUNK, maxMessageBytes);

  peer.id = id;
  peer.via = via;
  peer.closed = false;

  peer.end = (reason) => {
    if (peer.closed) return;
    peer.closed = true;
    shutdown();
    peer.emit('close', reason);
  };

  /** One frame in, from either channel : false once the peer is over its receive cap (and cut). */
  const admit = () => {
    const now = Date.now();
    tokens = Math.min(burst, tokens + ((now - stamp) * rate) / 1000);
    stamp = now;
    if (tokens < 1) {
      peer.end('peer-rate');
      return false;
    }
    tokens--;
    return true;
  };

  peer.receive = (frame) => {
    if (!admit()) return;
    const problem = assemble(frame);
    if (problem !== null) peer.end(problem);
  };

  peer.receiveUnreliable = (frame) => {
    if (!admit()) return;
    if (!isWhole(frame)) return peer.end('malformed');
    const problem = assembleUnreliable(frame);
    if (problem !== null) peer.end(problem);
  };

  peer.send = (data, options) => {
    if (peer.closed) return Promise.reject(new SignalError('closed', `peer ${id} is closed`));
    const conv = toBytes(data);
    if (conv === null) return Promise.reject(new SignalError('options', 'send() takes a string, a Uint8Array or an ArrayBuffer'));
    const bytes = conv.bytes;
    const unreliable = options?.reliable === false;
    const most = unreliable ? unreliableMax : maxMessageBytes;
    if (bytes.length > most) return Promise.reject(new SignalError('too-big', `${bytes.length} bytes > ${unreliable ? `${most}, one frame (reliable: false)` : `maxMessageBytes ${most}`}`));
    if (unreliable && sendUnreliable !== undefined) return Promise.resolve(sendUnreliable(encodeFrames(bytes, conv.text)[0]));
    const frames = encodeFrames(bytes, conv.text, chunk);
    const next = outbox.then(async () => {
      for (const frame of frames) {
        if (peer.closed) throw new SignalError('closed', `peer ${id} closed while sending`);
        await sendFrame(frame);
      }
    });
    outbox = next.catch(() => {});
    return unreliable ? next.then(() => true) : next;
  };

  peer.close = () => peer.end('closed');

  return peer;
}

/** A direct peer on its two open DataChannels : `channel` (reliable, ordered) and `unreliable`. */
export function directPeer({ id, pc, channel, unreliable, maxMessageBytes, limits }) {
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
    sendUnreliable: (frame) => {
      if (unreliable.readyState !== 'open' || unreliable.bufferedAmount > UNRELIABLE_HIGH) return false;
      unreliable.send(frame);
      return true;
    },
    shutdown: () => {
      try { channel.close(); } catch { /* already */ }
      try { unreliable.close(); } catch { /* already */ }
      pc.close();
    },
  });
  // After the proof the channel carries binary frames only : text here is a protocol violation.
  channel.addEventListener('message', ({ data }) => {
    if (typeof data === 'string') return peer.end('malformed');
    peer.receive(new Uint8Array(data));
  });
  channel.addEventListener('close', () => peer.end('closed'));
  unreliable.addEventListener('message', ({ data }) => {
    if (typeof data === 'string') return peer.end('malformed');
    peer.receiveUnreliable(new Uint8Array(data));
  });
  unreliable.addEventListener('close', () => peer.end('closed'));
  pc.addEventListener('connectionstatechange', () => {
    const state = pc.connectionState;
    if (state === 'failed' || state === 'closed' || state === 'disconnected') peer.end('closed');
  });
  return peer;
}
