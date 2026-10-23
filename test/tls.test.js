// TLS : wss:// served by the relay itself, and certificate renewal in place (bin/signal.js as a process).
import { strict as assert } from 'node:assert';
import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { connect as tlsConnect } from 'node:tls';
import { tmpdir } from 'node:os';
import { join as joinPath } from 'node:path';
import { after, before, describe, test } from 'node:test';
import { CLOSE, createRelay } from '../src/relay.js';
import { connect, join, quiet, roomId, spawnRelay } from './helpers.js';

const KEY = 'k3y-for-tests';

describe('TLS', () => {
  let dir;
  before(() => {
    dir = mkdtempSync(joinPath(tmpdir(), 'signal-relay-tls-'));
    const makeCert = (name, cn) => execFileSync('openssl', ['req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes',
      '-keyout', joinPath(dir, `${name}key.pem`), '-out', joinPath(dir, `${name}cert.pem`), '-days', '1', '-subj', `/CN=${cn}`], { stdio: 'ignore' });
    makeCert('', 'localhost');
    makeCert('a-', 'cert-a');
    makeCert('b-', 'cert-b');
  });
  after(() => rmSync(dir, { recursive: true, force: true }));

  test('serves wss:// itself with TLS_CERT / TLS_KEY, key check included', async (t) => {
    const relay = await createRelay({
      port: 0, host: '127.0.0.1', key: KEY, log: quiet,
      tls: { cert: readFileSync(joinPath(dir, 'cert.pem')), key: readFileSync(joinPath(dir, 'key.pem')) },
    });
    t.after(() => relay.close());
    const url = `wss://127.0.0.1:${relay.port}`;
    const ca = { ca: readFileSync(joinPath(dir, 'cert.pem')), servername: 'localhost' };
    const a = await join(url, { v: 1, id: 't1', room: roomId(), key: KEY }, ca);
    assert.equal(a.welcome.type, 'welcome');
    const bad = connect(url, ca);
    await bad.opened;
    bad.send({ type: 'hello', v: 1, id: 't2', room: roomId(), key: 'wrong' });
    assert.equal(await bad.closed, CLOSE.key);
    a.close();
  });

  // The CN of the certificate a fresh TLS handshake gets.
  const handshakeCn = (port) => new Promise((resolve, reject) => {
    const socket = tlsConnect({ port, host: '127.0.0.1', rejectUnauthorized: false }, () => {
      resolve(socket.getPeerCertificate().subject.CN);
      socket.end();
    });
    socket.once('error', reject);
  });

  // bin/signal.js as a process, as in the image : TLS_CERT / TLS_KEY files, renewed in place.
  async function startProcess(extraEnv) {
    const live = { cert: joinPath(dir, 'live-cert.pem'), key: joinPath(dir, 'live-key.pem') };
    copyFileSync(joinPath(dir, 'a-cert.pem'), live.cert);
    copyFileSync(joinPath(dir, 'a-key.pem'), live.key);
    const relay = await spawnRelay({ RELAY_KEY: KEY, TLS_CERT: live.cert, TLS_KEY: live.key, ...extraEnv });
    relay.swapToB = () => {
      copyFileSync(joinPath(dir, 'b-key.pem'), live.key);
      copyFileSync(joinPath(dir, 'b-cert.pem'), live.cert);
    };
    return relay;
  }

  for (const [how, extraEnv] of [['SIGHUP', { TLS_WATCH_MS: '0' }], ['a change of the files', { TLS_WATCH_MS: '100' }]]) {
    test(`reloads a renewed certificate on ${how} : new handshakes get it, open sockets and rooms stay up`, async (t) => {
      const relay = await startProcess(extraEnv);
      t.after(relay.stop);
      const url = `wss://127.0.0.1:${relay.port}`;
      const insecure = { rejectUnauthorized: false };
      const room = roomId();
      assert.equal(await handshakeCn(relay.port), 'cert-a');
      const a = await join(url, { v: 1, id: 'r1', room, key: KEY }, insecure);
      assert.equal(a.ws._socket.getPeerCertificate().subject.CN, 'cert-a');
      await a.next('peers'); // its own arrival

      relay.swapToB();
      if (how === 'SIGHUP') relay.child.kill('SIGHUP');
      await relay.waitLog(/tls reloaded/);
      assert.equal(await handshakeCn(relay.port), 'cert-b', 'a new handshake sees the renewed certificate');

      const b = await join(url, { v: 1, id: 'r2', room, key: KEY }, insecure);
      assert.equal(b.ws._socket.getPeerCertificate().subject.CN, 'cert-b');
      assert.deepEqual((await a.next('peers')).list.map((p) => p.id).sort(), ['r1', 'r2'], 'the socket opened under cert A is still in its room');
      a.send({ type: 'data', to: 'r2', box: 'c3RpbGwgdXA' });
      assert.deepEqual(await b.next('data'), { type: 'data', from: 'r1', box: 'c3RpbGwgdXA' });
      a.close(); b.close();
    });
  }

  test('a key that does not match the certificate is refused, the previous pair stays', async (t) => {
    const relay = await startProcess({ TLS_WATCH_MS: '0' });
    t.after(relay.stop);
    copyFileSync(joinPath(dir, 'b-cert.pem'), joinPath(dir, 'live-cert.pem')); // cert B, key still A
    relay.child.kill('SIGHUP');
    await relay.waitLog(/tls reload failed/);
    assert.equal(await handshakeCn(relay.port), 'cert-a');
  });
});
