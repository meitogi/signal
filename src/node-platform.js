// Node's platform : werift's RTCPeerConnection behind the W3C surface the core uses, and a `ws`
// WebSocket that remembers the HTTP status of a refused upgrade (a browser socket never sees it).
import { RTCPeerConnection as WeriftPeerConnection } from 'werift';
import WebSocket from 'ws';
import { MAX_MESSAGE_SIZE } from './sdp.js';

/** werift Event → addEventListener / removeEventListener, with `{ once }`. */
class Target {
  #subs = [];

  _bind(type, event, map) {
    this._events ??= Object.create(null);
    this._events[type] = { event, map };
  }

  addEventListener(type, fn, options) {
    const bound = this._events?.[type];
    if (!bound) return;
    const once = options === true || options?.once === true;
    const entry = { type, fn, sub: null };
    entry.sub = bound.event.subscribe((...args) => {
      const value = bound.map(...args);
      if (value === undefined) return;
      if (once) this.removeEventListener(type, fn);
      fn(value);
    });
    this.#subs.push(entry);
  }

  removeEventListener(type, fn) {
    const subs = this.#subs;
    for (let i = subs.length - 1; i >= 0; i--) {
      const s = subs[i];
      if (s.type === type && s.fn === fn) {
        s.sub.unSubscribe();
        subs.splice(i, 1);
      }
    }
  }
}

class NodeDataChannel extends Target {
  #dc;
  binaryType = 'arraybuffer';

  constructor(dc) {
    super();
    this.#dc = dc;
    this._bind('open', dc.stateChanged, (s) => (s === 'open' ? {} : undefined));
    this._bind('close', dc.stateChanged, (s) => (s === 'closed' ? {} : undefined));
    this._bind('bufferedamountlow', dc.bufferedAmountLow, () => ({}));
    this._bind('error', dc.error, (error) => ({ error }));
    this._bind('message', dc.onMessage, (data) => ({
      data: typeof data === 'string' ? data : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
    }));
  }

  get label() { return this.#dc.label; }
  get readyState() { return this.#dc.readyState; }
  get bufferedAmount() { return this.#dc.bufferedAmount; }
  get bufferedAmountLowThreshold() { return this.#dc.bufferedAmountLowThreshold; }
  set bufferedAmountLowThreshold(value) { this.#dc.bufferedAmountLowThreshold = value; }

  send(data) {
    if (typeof data === 'string') return this.#dc.send(data);
    if (data instanceof ArrayBuffer) return this.#dc.send(Buffer.from(data));
    this.#dc.send(Buffer.from(data.buffer, data.byteOffset, data.byteLength));
  }

  close() { this.#dc.close(); }
}

export class NodePeerConnection extends Target {
  #pc;

  constructor({ iceServers = [] } = {}) {
    super();
    const pc = new WeriftPeerConnection({ iceServers, maxMessageSize: MAX_MESSAGE_SIZE });
    this.#pc = pc;
    this._bind('icecandidate', pc.onIceCandidate, (c) => ({ candidate: c ? c.toJSON() : null }));
    this._bind('connectionstatechange', pc.connectionStateChange, () => ({}));
    this._bind('datachannel', pc.onDataChannel, (dc) => ({ channel: new NodeDataChannel(dc) }));
  }

  get connectionState() { return this.#pc.connectionState; }
  get localDescription() { return this.#pc.localDescription; }
  get remoteDescription() { return this.#pc.remoteDescription; }

  createDataChannel(label, options) { return new NodeDataChannel(this.#pc.createDataChannel(label, options)); }
  createOffer() { return this.#pc.createOffer(); }
  createAnswer() { return this.#pc.createAnswer(); }
  setLocalDescription(desc) { return this.#pc.setLocalDescription(desc); }
  setRemoteDescription(desc) { return this.#pc.setRemoteDescription(desc); }
  addIceCandidate(candidate) { return this.#pc.addIceCandidate(candidate); }
  close() { this.#pc.close().catch(() => {}); }
}

/** `ws` with the W3C event API it already has, plus `httpStatus` when the upgrade was refused. */
export class NodeWebSocket extends WebSocket {
  httpStatus = undefined;

  constructor(url) {
    super(url);
    this.on('unexpected-response', (req, res) => {
      this.httpStatus = res.statusCode;
      res.resume();
      this.terminate(); // what `ws` does without a listener : 'error', then 'close' 1006
    });
  }
}

export const platform = { RTCPeerConnection: NodePeerConnection, WebSocket: NodeWebSocket };
