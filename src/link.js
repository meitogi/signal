// A room shared by a link : everything after `#`, which a browser never sends to the web server.
import { isSecret } from './crypto.js';

/**
 * @param {string} base - the page URL, with or without a fragment (replaced)
 * @param {{room: string, secret: string, relay?: string}} parts
 * @returns {string} `base#r=<room>&s=<secret>[&relay=<url>]`
 */
export function formatLink(base, { room, secret, relay }) {
  const params = new URLSearchParams();
  params.set('r', room);
  params.set('s', secret);
  if (relay) params.set('relay', relay);
  return `${String(base).split('#')[0]}#${params}`;
}

/**
 * @param {string} url - a full URL or just its `#…` part
 * @returns {{room: string, secret: string, relay?: string}|null} null unless both a room and a well-formed secret are there
 */
export function parseLink(url) {
  const text = String(url);
  const i = text.indexOf('#');
  if (i < 0) return null;
  const params = new URLSearchParams(text.slice(i + 1));
  const room = params.get('r');
  const secret = params.get('s');
  if (!room || !secret || !isSecret(secret)) return null;
  const relay = params.get('relay');
  return relay ? { room, secret, relay } : { room, secret };
}
