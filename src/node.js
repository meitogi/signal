// @meitogi/signal/node — the same API in Node ≥ 22 : WebRTC by werift (pure JS), WebSocket by `ws`.
import { platform } from './node-platform.js';
import { createClient } from './room.js';

export { SignalError } from './errors.js';
export { newSecret } from './crypto.js';
export { formatLink, parseLink } from './link.js';

export const { joinRoom } = createClient(platform);
