// Pure parts : secrets, links, option validation before any network, close-code mapping.
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { isSecret, newSecret } from '../src/crypto.js';
import { CLOSE_CODES, closeError, SignalError } from '../src/errors.js';
import { formatLink, parseLink } from '../src/link.js';
import { createClient } from '../src/room.js';
import { once } from './client-helpers.js';

test('newSecret : 43 base64url chars of 32 bytes, unique', () => {
  const a = newSecret();
  const b = newSecret();
  assert.match(a, /^[A-Za-z0-9_-]{43}$/);
  assert.notEqual(a, b);
  assert.equal(isSecret(a), true);
  assert.equal(isSecret(a.slice(1)), false);
  assert.equal(isSecret(`${a.slice(0, 42)}+`), false);
  assert.equal(isSecret(42), false);
});

test('formatLink / parseLink round-trip, fragment replaced, relay optional', () => {
  const secret = newSecret();
  const link = formatLink('https://app.example/p?x=1#old', { room: 'partie du soir', secret });
  assert.equal(link, `https://app.example/p?x=1#r=partie+du+soir&s=${secret}`);
  assert.deepEqual(parseLink(link), { room: 'partie du soir', secret });
  const withRelay = formatLink('https://app.example/', { room: 'r', secret, relay: 'wss://relay.example' });
  assert.deepEqual(parseLink(withRelay), { room: 'r', secret, relay: 'wss://relay.example' });
  assert.deepEqual(parseLink(`#r=r&s=${secret}`), { room: 'r', secret });
  assert.equal(parseLink('https://app.example/'), null);
  assert.equal(parseLink(`https://app.example/#r=r&s=${secret.slice(2)}`), null, 'a malformed secret is not a link');
  assert.equal(parseLink(`https://app.example/#s=${secret}`), null, 'no room');
});

test('formatLink / parseLink : an optional relay key travels as `key`, read back as relayKey', () => {
  const secret = newSecret();
  const link = formatLink('https://app.example/', { room: 'r', secret, relay: 'wss://relay.example', relayKey: 'k-123_ABC' });
  assert.equal(link, `https://app.example/#r=r&s=${secret}&relay=wss%3A%2F%2Frelay.example&key=k-123_ABC`);
  assert.deepEqual(parseLink(link), { room: 'r', secret, relay: 'wss://relay.example', relayKey: 'k-123_ABC' });
  assert.deepEqual(parseLink(`#r=r&s=${secret}&key=abc`), { room: 'r', secret, relayKey: 'abc' });
  assert.deepEqual(parseLink(`#r=r&s=${secret}&key=`), { room: 'r', secret }, 'an empty key is no key');
  assert.equal(formatLink('https://app.example/', { room: 'r', secret, relayKey: '' }), `https://app.example/#r=r&s=${secret}`);
});

test('joinRoom : invalid options reject ready and close the room without touching the network', async () => {
  let sockets = 0;
  const { joinRoom } = createClient({
    RTCPeerConnection: class { constructor() { throw new Error('no RTCPeerConnection expected'); } },
    WebSocket: class { constructor() { sockets++; throw new Error('no socket expected'); } },
  });
  const secret = newSecret();
  const cases = [
    [{ relay: 'ws://relay.example', room: 'r', secret }, 'options'],
    [{ relay: 'http://localhost', room: 'r', secret }, 'options'],
    [{ relay: 'not a url', room: 'r', secret }, 'options'],
    [{ room: 'r', secret }, 'options'],
    [{ relay: 'wss://relay.example', room: '', secret }, 'options'],
    [{ relay: 'wss://relay.example', room: 'x'.repeat(65), secret }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, max: 1 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, max: '4' }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, connectMs: 0 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, iceServers: 'stun' }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, sendRate: 0 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, sendRate: 1.5 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, sendRate: '100' }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, sendBurst: -1 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r', secret, sendBurst: 0 }, 'options'],
    [{ relay: 'wss://relay.example', room: 'r' }, 'secret'],
    [{ relay: 'wss://relay.example', room: 'r', secret: 'short' }, 'secret'],
    [{ relay: 'ws://localhost:8787', room: 'r', secret: `${secret.slice(0, 42)}=` }, 'secret'],
  ];
  for (const [options, code] of cases) {
    const room = joinRoom(options);
    const closed = once(room, 'close');
    await assert.rejects(room.ready, (e) => e instanceof SignalError && e.code === code && e.fatal === true, JSON.stringify(options));
    const [error] = await closed;
    assert.equal(error.code, code);
    assert.deepEqual(room.peers, []);
  }
  assert.equal(sockets, 0);
});

test('close codes map to the contract, fatal or not ; 429 is banned only before entry', () => {
  assert.deepEqual(
    Object.fromEntries(Object.keys(CLOSE_CODES).map((c) => [c, [CLOSE_CODES[c].code, CLOSE_CODES[c].fatal]])),
    { 4400: ['relay', true], 4401: ['key', true], 4403: ['room-full', true], 4408: ['relay', false], 4409: ['id-taken', false], 4429: ['rate', true], 1009: ['relay', true] },
  );
  const e = closeError(1006, undefined, false);
  assert.equal(e.code, 'relay');
  assert.equal(e.fatal, false);
  assert.equal(closeError(1006, 429, false).code, 'banned');
  assert.equal(closeError(1006, 429, false).fatal, true);
  assert.equal(closeError(1006, 429, true).code, 'relay');
  assert.equal(closeError(4401, undefined, true).code, 'key');
  assert.equal(closeError(4403, undefined, true).fatal, true, '4403 on a reconnect is fatal too');
  assert.equal(new SignalError('too-big', 'x').fatal, false);
  assert.equal(new SignalError('key', 'x').fatal, true);
});
