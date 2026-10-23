// @meitogi/signal — browser entry. Relative imports only : no dependency, no Node built-in.
import { createClient } from './room.js';

export { SignalError } from './errors.js';
export { newSecret } from './crypto.js';
export { formatLink, parseLink } from './link.js';

export const { joinRoom } = createClient({
  RTCPeerConnection: globalThis.RTCPeerConnection,
  WebSocket: globalThis.WebSocket,
});
