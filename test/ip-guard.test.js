// The IP guard on its own : X-Forwarded-For parsing, slot accounting, the bounded IP table.
import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { createGuard, parseTrust } from '../src/ip-guard.js';
import { quiet } from './helpers.js';

const make = (options) => createGuard({ ipConn: 10, pendingMax: 200, connMax: 2000, banAfter: 5, banMs: 600_000, trust: null, log: quiet, ...options });
const req = (remoteAddress, xff) => ({ socket: { remoteAddress }, headers: xff === undefined ? {} : { 'x-forwarded-for': xff } });

test('X-Forwarded-For : read right to left behind a trusted proxy only, trusted hops skipped', (t) => {
  const guard = make({ trust: parseTrust('127.0.0.1, 10.0.0.0/8') });
  t.after(guard.close);
  assert.equal(guard.ipOf(req('::ffff:127.0.0.1', '203.0.113.7')), '203.0.113.7');
  assert.equal(guard.ipOf(req('127.0.0.1', '6.6.6.6, 203.0.113.7')), '203.0.113.7', 'the leftmost entry is client-written : not believed');
  assert.equal(guard.ipOf(req('127.0.0.1', '203.0.113.7, 10.1.2.3')), '203.0.113.7', 'a second trusted proxy is skipped');
  assert.equal(guard.ipOf(req('127.0.0.1', 'junk, 203.0.113.7')), '203.0.113.7');
  assert.equal(guard.ipOf(req('127.0.0.1', '203.0.113.7, junk')), '127.0.0.1', 'a malformed last hop : the socket IP stays');
  assert.equal(guard.ipOf(req('127.0.0.1')), '127.0.0.1');
  assert.equal(guard.ipOf(req('192.0.2.1', '203.0.113.7')), '192.0.2.1', 'an untrusted source cannot pick its IP');
  assert.throws(() => parseTrust('10.0.0.0/33'));
  assert.throws(() => parseTrust('proxy.local'));
});

test('slots : pending, total and per-IP counts come back to zero', (t) => {
  const guard = make({ ipConn: 2, pendingMax: 3, connMax: 4 });
  t.after(guard.close);
  const a = guard.admit('192.0.2.1');
  const b = guard.admit('192.0.2.1');
  assert.equal(guard.admit('192.0.2.1'), 'ip');
  const c = guard.admit('192.0.2.2');
  assert.equal(guard.admit('192.0.2.3'), 'pending');
  guard.helloed(a);
  guard.helloed(a); // twice : counted once
  guard.helloed(b);
  const d = guard.admit('192.0.2.3');
  assert.equal(guard.admit('192.0.2.4'), 'total');
  for (const slot of [a, b, c, d]) guard.release(slot);
  const s = guard.stats();
  assert.deepEqual([s.pending, s.total], [0, 0]);
  assert.equal(s.ips, 0, 'a per-IP refusal is no fault : nothing to remember once released');
});

test('a full IP is refused, never banned for it : its next socket gets in once one leaves', (t) => {
  const guard = make({ ipConn: 1, banAfter: 1 });
  t.after(guard.close);
  const held = guard.admit('192.0.2.1');
  for (let i = 0; i < 20; i++) assert.equal(guard.admit('192.0.2.1'), 'ip');
  guard.release(held);
  const next = guard.admit('192.0.2.1');
  assert.equal(typeof next, 'object', `got ${next} : refusals at the cap counted as faults`);
  assert.equal(next.ip, '192.0.2.1');
  guard.release(next);
});

test('IP table : a flood of IPs with one fault each stays bounded, bans are kept', (t) => {
  const guard = make({ banAfter: 2 });
  t.after(guard.close);
  const banned = '198.51.100.1';
  for (let i = 0; i < 2; i++) {
    const slot = guard.admit(banned);
    guard.fault(banned);
    guard.release(slot);
  }
  assert.equal(guard.admit(banned), 'banned');
  for (let i = 0; i < 30_000; i++) {
    const ip = `10.${(i >> 16) & 255}.${(i >> 8) & 255}.${i & 255}`;
    const slot = guard.admit(ip);
    guard.fault(ip);
    guard.release(slot);
  }
  const s = guard.stats();
  assert.ok(s.ips <= 10_001, `${s.ips} IPs remembered`);
  assert.deepEqual([s.pending, s.total], [0, 0]);
  assert.equal(guard.admit(banned), 'banned', 'the ban survived the flood');
});
