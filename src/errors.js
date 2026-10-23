// Every error the client raises is a SignalError with a `code` from CONTRACT.md « Erreurs ».
// `fatal` : the room is over (`ready` rejected, or `close` emitted) ; otherwise `error` is emitted
// and the room goes on.

/** Codes that end the room. The others are incidents on one peer, or `send()` rejections. */
export const FATAL = { options: true, secret: true, key: true, 'room-full': true, rate: true, banned: true };

export class SignalError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {boolean} [fatal] - defaults to the code's nature (FATAL)
   */
  constructor(code, message, fatal = FATAL[code] === true) {
    super(message);
    this.name = 'SignalError';
    this.code = code;
    this.fatal = fatal;
  }
}

/**
 * Relay close code → what the client makes of it. The same table is in README.md.
 * - 4400 (bad hello) and 1009 (frame over RELAY_MAX_BYTES) cannot happen by construction : a
 *   protocol mismatch, fatal `relay` — reconnecting would only repeat the fault (and a fault counts
 *   toward an IP ban).
 * - 4401 covers a wrong / missing `relayKey` AND a hello that reached the relay late : the client
 *   cannot tell them apart ; both are fatal `key`.
 * - 4403 is fatal at entry and on a reconnect alike : our slot was taken while we were away.
 * - 4408 (idle) : pings every 5 s make it rare (a laptop asleep) ; a plain reconnect.
 * - 4409 : a new id is drawn, then a reconnect.
 * - 4429 : rate, or too many offers to one peer — stays out of reach by construction.
 * Anything else (1006 network, 1001 / 1012 relay going away, no connection at all) is a
 * non-fatal `relay` : the room reconnects with backoff.
 */
export const CLOSE_CODES = {
  4400: { code: 'relay', fatal: true, message: 'the relay refused our hello (protocol mismatch)' },
  4401: { code: 'key', fatal: true, message: 'the relay refused the key, or the hello arrived late' },
  4403: { code: 'room-full', fatal: true, message: 'the room is full' },
  4408: { code: 'relay', fatal: false, message: 'the relay closed an idle socket' },
  4409: { code: 'id-taken', fatal: false, message: 'our id is taken in the room ; drawing a new one' },
  4429: { code: 'rate', fatal: true, message: 'cut by the relay for rate (messages or offers)' },
  1009: { code: 'relay', fatal: true, message: 'the relay refused a frame as too big (protocol mismatch)' },
};

/**
 * The SignalError for a socket that closed (or never opened).
 * @param {number} wsCode - WebSocket close code (1006 when the socket never opened)
 * @param {number|undefined} httpStatus - the HTTP status of a refused upgrade, when the platform
 *   can see it (Node `ws`) ; a browser WebSocket never can
 * @param {boolean} entered - true once this room was welcomed at least once
 */
export function closeError(wsCode, httpStatus, entered) {
  // A 429 at upgrade is a ban OR a full connection cap : indistinguishable. Before we ever got in
  // we call it a ban (a retry under a ban is pointless) ; once a member, we keep retrying.
  if (httpStatus === 429 && !entered) return new SignalError('banned', 'the relay refused the connection (HTTP 429 : banned or full)', true);
  const known = CLOSE_CODES[wsCode];
  if (known) return new SignalError(known.code, known.message, known.fatal);
  return new SignalError('relay', `relay unreachable or closed (${wsCode})`, false);
}
