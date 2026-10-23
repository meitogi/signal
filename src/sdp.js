// What a peer proves knowledge of the secret over, and the one line we change in our own SDP.

/** What our SDP advertises as the largest DataChannel message we accept (werift's own limit). */
export const MAX_MESSAGE_SIZE = 65536;

/**
 * The DTLS fingerprints an SDP commits to, normalised : every `a=fingerprint:` line, algorithm
 * lowercase, value uppercase, deduplicated, sorted, joined by `,`. DTLS refuses a peer whose
 * certificate does not match the SDP that was applied, so a proof over these values binds to the
 * actual encrypted connection.
 */
export function fingerprints(sdp) {
  const found = [];
  for (const line of sdp.split(/\r?\n/)) {
    const m = /^a=fingerprint:(\S+)\s+(\S+)/.exec(line.trim());
    if (!m) continue;
    const value = `${m[1].toLowerCase()} ${m[2].toUpperCase()}`;
    if (!found.includes(value)) found.push(value);
  }
  return found.sort().join(',');
}

/**
 * Each side proves the secret over : its role, both SDPs' fingerprints, the room id, and one fresh
 * nonce per side (a proof from another connection does not replay).
 * @param {'offerer'|'answerer'} role
 */
export function transcript(role, { offerSdp, answerSdp, roomId, offerNonce, answerNonce }) {
  return ['meitogi-signal auth v1', role, fingerprints(offerSdp), fingerprints(answerSdp), roomId, offerNonce, answerNonce].join('\n');
}

/**
 * Our SDP with `a=max-message-size` lowered to MAX_MESSAGE_SIZE : a peer cannot make us buffer a
 * 256 KB SCTP message (Chrome's default) — our frames are 16 KB anyway.
 */
export function capMessageSize(sdp) {
  if (/^a=max-message-size:/m.test(sdp)) return sdp.replace(/^a=max-message-size:\d+/gm, `a=max-message-size:${MAX_MESSAGE_SIZE}`);
  return sdp.replace(/^(a=sctp-port:\d+)(\r?\n)/m, `$1$2a=max-message-size:${MAX_MESSAGE_SIZE}$2`);
}
