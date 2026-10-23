// Who may open a socket : the real client IP (X-Forwarded-For only from a trusted proxy),
// the per-IP / pending / total connection caps checked at `upgrade`, and the IP ban table.
// Everything here is plain counters on a bounded table : a flood costs a lookup, never a socket.
import { BlockList, isIP } from 'node:net';

const FAULT_WINDOW_MS = 10 * 60_000; // RELAY_BAN_AFTER faults within this window → ban
const LEVEL_MEMORY_MS = 24 * 3600_000; // a ban level is forgotten this long after the ban ends
const TABLE_MAX = 10_000; // past this many IPs, one leaving with faults but no ban history is forgotten
const SWEEP_MS = 60_000;

/** `::ffff:1.2.3.4` → `1.2.3.4`, so a v4 client is one key whatever the socket family. */
const normalize = (ip) => (ip.startsWith('::ffff:') && isIP(ip.slice(7)) === 4 ? ip.slice(7) : ip);

/**
 * Parses `TRUST_PROXY` : comma-separated IPs or CIDRs. Throws on a malformed entry.
 * @param {string} spec
 * @returns {BlockList|null} null when empty
 */
export function parseTrust(spec) {
  const entries = String(spec ?? '').split(',').map((s) => s.trim()).filter(Boolean);
  if (!entries.length) return null;
  const list = new BlockList();
  for (const entry of entries) {
    const [addr, prefix] = entry.split('/');
    const family = isIP(addr);
    if (!family) throw new Error(`TRUST_PROXY : not an IP or CIDR : ${entry}`);
    const type = family === 6 ? 'ipv6' : 'ipv4';
    if (prefix === undefined) {
      list.addAddress(addr, type);
      continue;
    }
    const bits = Number(prefix);
    if (!Number.isInteger(bits) || bits < 0 || bits > (family === 6 ? 128 : 32)) throw new Error(`TRUST_PROXY : bad prefix : ${entry}`);
    list.addSubnet(addr, bits, type);
  }
  return list;
}

/**
 * @param {object} options
 * @param {number} options.ipConn - concurrent sockets per IP
 * @param {number} options.pendingMax - sockets without a hello, all IPs
 * @param {number} options.connMax - sockets, all IPs
 * @param {number} options.banAfter - faults within 10 min before a ban
 * @param {number} options.banMs - first ban ; doubled on each repeat
 * @param {BlockList|null} options.trust - proxies whose X-Forwarded-For is believed
 * @param {(line: string) => void} options.log
 */
export function createGuard({ ipConn, pendingMax, connMax, banAfter, banMs, trust, log }) {
  /** ip → {conns, faults: number[], until, level, forgetAt} */
  const table = Object.create(null);
  let size = 0;
  let pending = 0;
  let total = 0;
  const refused = { banned: 0, ip: 0, pending: 0, total: 0 };

  const trusted = (ip) => trust !== null && trust.check(ip, isIP(ip) === 6 ? 'ipv6' : 'ipv4');

  function entry(ip, create) {
    let e = table[ip];
    if (e === undefined && create) {
      e = table[ip] = { conns: 0, faults: [], until: 0, level: 0, forgetAt: 0 };
      size++;
    }
    return e;
  }

  function idle(e, now) {
    if (e.conns > 0 || e.until > now || e.forgetAt > now) return false;
    const faults = e.faults;
    return faults.length === 0 || faults[faults.length - 1] <= now - FAULT_WINDOW_MS;
  }

  function drop(ip) {
    delete table[ip];
    size--;
  }

  /** Records a fault ; the `banAfter`-th within the window bans the IP. */
  function fault(ip) {
    const now = Date.now();
    const e = table[ip]; // every fault comes from a socket of this IP : its entry exists
    if (e === undefined) return;
    const faults = e.faults;
    while (faults.length && faults[0] <= now - FAULT_WINDOW_MS) faults.shift();
    faults.push(now);
    if (faults.length < banAfter) return;
    const ms = banMs * 2 ** e.level;
    e.until = now + ms;
    e.forgetAt = e.until + LEVEL_MEMORY_MS;
    e.level++;
    e.faults = [];
    log(`  ban ${ip} for ${Math.round(ms / 1000)} s (level ${e.level})`);
  }

  const sweep = setInterval(() => {
    const now = Date.now();
    for (const ip in table) if (idle(table[ip], now)) drop(ip);
  }, SWEEP_MS);
  sweep.unref();

  return {
    /** The client's IP : the socket's, or the nearest untrusted X-Forwarded-For hop behind a trusted proxy. */
    ipOf(req) {
      let ip = normalize(req.socket.remoteAddress ?? '');
      if (!trusted(ip)) return ip;
      const header = req.headers['x-forwarded-for'];
      if (typeof header !== 'string') return ip;
      const hops = header.split(',');
      for (let i = hops.length - 1; i >= 0; i--) {
        const hop = normalize(hops[i].trim());
        if (!isIP(hop)) break;
        ip = hop;
        if (!trusted(hop)) break;
      }
      return ip;
    },

    /**
     * Takes a slot for a new socket from `ip`, or says why not.
     * @returns {{ip: string, pending: boolean}|string} the slot, or 'banned' | 'ip' | 'pending' | 'total'
     */
    admit(ip) {
      const e = table[ip];
      let reason = null;
      if (e !== undefined && e.until > Date.now()) reason = 'banned';
      else if (e !== undefined && e.conns >= ipConn) reason = 'ip';
      else if (pending >= pendingMax) reason = 'pending';
      else if (total >= connMax) reason = 'total';
      if (reason !== null) {
        refused[reason]++;
        if (reason === 'ip') fault(ip);
        return reason;
      }
      entry(ip, true).conns++;
      pending++;
      total++;
      return { ip, pending: true };
    },

    /** The slot's socket sent a valid hello : it no longer counts as pending. */
    helloed(slot) {
      if (!slot.pending) return;
      slot.pending = false;
      pending--;
    },

    /** The slot's socket is gone. */
    release(slot) {
      if (slot.pending) pending--;
      slot.pending = false;
      total--;
      const e = table[slot.ip];
      if (e === undefined) return;
      e.conns--;
      const now = Date.now();
      // Many IPs with a fault or two (a flood from spread addresses) : keep only bans and their
      // levels once the table is full, so it stays bounded by TABLE_MAX + live connections.
      if (idle(e, now) || (size > TABLE_MAX && e.conns === 0 && e.until <= now && e.level === 0)) drop(slot.ip);
    },

    fault,

    stats: () => ({ pending, total, ips: size, refused: { ...refused } }),

    close: () => clearInterval(sweep),
  };
}
