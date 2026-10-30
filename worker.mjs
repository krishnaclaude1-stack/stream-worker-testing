#!/usr/bin/env node
/**
 * Vote for one or more contestants in the TamilGlitz "Bigg Boss Tamil Vote" poll
 * (Crowdsignal/Polldaddy embed on https://tamilglitz.in/bigg-boss-tamil-vote/).
 *
 *   node worker.mjs --target 500                         # equal split across the default three
 *   node worker.mjs "Lakshmi Priya" "Jason" "Muniyammal" # explicit names
 *   node worker.mjs --names "Lakshmi Priya,Jason,Muniyammal" --dry-run
 *
 * The poll counts ONE vote per egress IP per ~3h window, so N votes need N IPs.
 * Verified: the server ignores the `b`/`cookie` params, so there is no client-side bypass —
 * distinct IPs are the only lever.
 *
 *   Tor (one fresh circuit per vote; measured 85% hit rate, 2.5 votes/min):
 *     node worker.mjs --tor --target 300
 *     node worker.mjs --tor --times 10
 *
 *   Tor with parallel isolated circuits (IsolateSOCKSAuth: every attempt uses an unused
 *   SOCKS username, so it gets its own circuit — no control port, no NEWNYM, no 10s limit):
 *     node worker.mjs --tor-streams 12 --target 300
 *
 * Above concurrency 1 a slice of accepted votes never reaches the tally: two circuits can exit
 * from the same IP and race the server's per-IP dedupe, so both answer "vote counted" while only
 * one is recorded (measured ~26% lost at 10-16 lanes, 0% at 1). Judge a run by the poll's own
 * counter, not the "counted" line — --tally-each prints it on every vote.
 *
 *   A proxy list you supply — one "host:port", "http://user:pass@host:port" or
 *   "socks5://host:port" per line:
 *     node worker.mjs --proxies proxies.txt --target 300 --concurrency 30
 *     HTTPS_PROXY=http://user:pass@host:port node worker.mjs
 *
 *   ProxyScrape's live free list (http + socks4 + socks5), fetched and validated in-process:
 *     node worker.mjs --proxies proxyscrape --target 300
 *     node worker.mjs --proxies proxyscrape --proxy-protocols http,socks5 --no-validate
 *
 *   Both: the list is drained first, Tor carries the tail.
 *
 * --target N stops once N votes are counted total, split as evenly as possible across
 * the listed names (500 with three names -> 167/167/166). --times is the attempt cap.
 * --ledger  path to the spent-IP file (default spent-<pollId>.txt).
 *
 * Free lists are mostly dead or plain-HTTP-only, so every scraped proxy is probed once
 * (HTTPS GET through the tunnel) and only those that return an egress IP are used. The probe
 * also dedupes proxies that share an exit IP, which the poll would reject as "already counted".
 * --no-validate skips the probe; --validate-concurrency / --validate-timeout tune it.
 * --proxy-protocols defaults to http,socks5,socks4; socks4 is spoken as SOCKS4a.
 * Every IP that returns "already counted" is appended to the ledger and skipped on later runs.
 *
 * --tor auto-detects the SOCKS and control ports (9050/9051, then Tor Browser's 9150/9151),
 * authenticates to the control port via TOR_COOKIE / TOR_CONTROL_PASSWORD / empty password,
 * and sends SIGNAL NEWNYM before every vote. Tor rate-limits NEWNYM to one per 10s, so
 * --tor alone forces concurrency 1 and an 11s delay unless you override both explicitly.
 */

import https from "node:https";
import http from "node:http";
import tls from "node:tls";
import net from "node:net";
import { readFileSync, appendFileSync, existsSync, statSync } from "node:fs";

const PAGE = process.env.POLL_PAGE ?? "https://tamilglitz.in/bigg-boss-tamil-vote/";
// Short UA on purpose: the full Chrome UA trips Cloudflare's bot rule on the blog page.
const UA = process.env.POLL_UA ?? "Mozilla/5.0 (Windows NT 10.0; Win64; x64)";
const TIMEOUT = Number(process.env.POLL_TIMEOUT ?? 15000);

// Multi-hour runs must survive a single wedged socket or stray rejection.
process.on("unhandledRejection", (e) => console.error(`unhandled rejection: ${e?.message ?? e}`));
process.on("uncaughtException", (e) => console.error(`uncaught exception: ${e?.message ?? e}`));

const argv = process.argv.slice(2);
const flag = (n, d) => { const i = argv.indexOf(n); return i < 0 ? d : argv[i + 1]; };
const has = (n) => argv.includes(n);
const skipNext = new Set(["--times", "--delay", "--proxies", "--concurrency", "--target", "--ledger", "--tor-streams",
  "--proxy-protocols", "--validate-timeout", "--validate-concurrency", "--max-proxies", "--names"]);
const namesFlag = flag("--names", null);
const positionalNames = argv.filter((a, i) => !a.startsWith("--") && !skipNext.has(argv[i - 1]));
const requestedNames = namesFlag
  ? namesFlag.split(",").map((s) => s.trim()).filter(Boolean)
  : positionalNames.length ? positionalNames : ["Lakshmi Priya", "Jason", "Muniyammal"];
const names = [...new Map(requestedNames.map((n) => [n.toLowerCase(), n])).values()];
if (!names.length) {
  console.error("no ballot names supplied");
  process.exit(1);
}

const torMode = has("--tor");
const streams = Math.max(0, Number(flag("--tor-streams", 0)));   // parallel isolated Tor circuits
const tallyEach = has("--tally-each");   // print each selected ballot's tally on every vote
const dryRun = has("--dry-run");
const proxySrc = flag("--proxies", null);
const envProxy = process.env.HTTPS_PROXY || process.env.https_proxy;
const hasProxies = Boolean(proxySrc || envProxy);
let times = Number(flag("--times", 1));
// proxies pace themselves; a bare --tor is throttled by Tor's 10s NEWNYM limit.
// --tor-streams is not: a fresh circuit per attempt needs no NEWNYM at all.
const delay = Number(flag("--delay", hasProxies || streams ? 0 : torMode ? 11000 : 300));
const concurrency = Math.max(1, Number(flag("--concurrency", streams || (hasProxies ? 20 : torMode ? 1 : 8))));
const target = Number(flag("--target", 0));   // stop as soon as N votes are actually counted
const ledgerFlag = flag("--ledger", null);    // override the spent-<pollId>.txt path (for A/B runs)

const VALID_PROTOCOLS = new Set(["http", "socks5", "socks4"]);
const proxyProtocols = String(flag("--proxy-protocols", "http,socks5,socks4"))
  .split(",").map((s) => s.trim().toLowerCase()).filter((s) => VALID_PROTOCOLS.has(s));
const validateProxies = !has("--no-validate");
const validateTimeout = Math.max(1000, Number(flag("--validate-timeout", 8000)));
const validateConcurrency = Math.max(1, Number(flag("--validate-concurrency", Math.min(200, Math.max(20, concurrency * 5)))));
const maxProxies = Math.max(0, Number(flag("--max-proxies", 0)));   // cap candidates before probing

/* ---------------------------------------------------------------- tunnelling */

// node:https, not fetch: Cloudflare 403s undici's TLS fingerprint but serves node:https (HTTP/1.1).
const normalize = (p) => new URL(/^\w+:\/\//.test(p) ? p : `http://${p}`);
const isSocks = (u) => /^socks/i.test(u.protocol);
const isSocks4 = (u) => /^socks4/i.test(u.protocol);
// Key on the egress IP: free lists carry the same host on many ports, and those extra
// attempts all land on one already-credited IP ("already counted" — measured 27/1800 wasted).
const key = (u) => (/^(?:\d{1,3}\.){3}\d{1,3}$/.test(u.hostname) || u.hostname.includes(":"))
  ? u.hostname
  : `${u.hostname}:${u.port || (isSocks(u) ? 1080 : 80)}`;

function withTimeout(sock, label, timeout = TIMEOUT) {
  sock.setTimeout(timeout, () => sock.destroy(new Error(`${label} timeout`)));
  return sock;
}

const waitConnect = (sock) => new Promise((resolve, reject) => {
  sock.once("connect", resolve);
  sock.once("error", reject);
});

// CONNECT tunnel. The proxy only moves the egress IP, so no TLS termination
// (MITM) is involved and no certificate pinning is bypassed anywhere.
async function httpTunnel(proxy, host, port, timeout = TIMEOUT) {
  const raw = await new Promise((resolve, reject) => {
    let settled = false;
    const ok = (s) => { settled = true; resolve(s); };
    const bad = (e) => { if (!settled) { settled = true; reject(e); } };
    const auth = proxy.username
      ? { "Proxy-Authorization": "Basic " + Buffer.from(`${decodeURIComponent(proxy.username)}:${decodeURIComponent(proxy.password)}`).toString("base64") }
      : {};
    const req = http.request({
      host: proxy.hostname,
      port: proxy.port || 80,
      method: "CONNECT",
      path: `${host}:${port}`,
      headers: auth,
      timeout,
    });
    req.on("connect", (res, socket) =>
      res.statusCode === 200 ? ok(socket) : bad(new Error(`proxy CONNECT ${res.statusCode}`)));
    // a proxy that accepts TCP then closes without answering emits neither connect nor error
    req.on("close", () => bad(new Error("proxy closed before CONNECT")));
    req.on("timeout", () => req.destroy(new Error("proxy connect timeout")));
    req.on("error", bad);
    req.end();
  });
  return raw;
}

// RFC 1928 CONNECT with the hostname sent as ATYP=3, so DNS resolves at the proxy
// (Tor and socks5h behaviour). RFC 1929 user/pass auth is supported.
function socks5Tunnel(proxy, host, port, timeout = TIMEOUT) {
  return new Promise((resolve, reject) => {
    const sock = withTimeout(net.connect({ host: proxy.hostname, port: Number(proxy.port || 1080) }), "socks", timeout);
    let stage = 0;
    let buf = Buffer.alloc(0);
    let settled = false;
    // stays attached after resolve: a late RST must not surface as an uncaught error
    const fail = (e) => { if (settled) return; settled = true; sock.destroy(); reject(e); };

    const sendConnect = () => {
      const h = Buffer.from(host);
      sock.write(Buffer.concat([Buffer.from([5, 1, 0, 3, h.length]), h, Buffer.from([(port >> 8) & 0xff, port & 0xff])]));
      stage = 2;
      buf = Buffer.alloc(0);
    };

    sock.on("error", () => {});   // a late reset after settle must not become an uncaughtException
    sock.once("error", fail);
    sock.once("close", () => fail(new Error("socks5 connection closed during handshake")));
    sock.once("connect", () => sock.write(Buffer.from(proxy.username ? [5, 2, 0, 2] : [5, 1, 0])));
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);

      if (stage === 0) {                                    // greeting
        if (buf.length < 2) return;
        const [ver, method] = buf;
        if (ver !== 5) return fail(new Error("socks5 bad version"));
        if (method === 0xff) return fail(new Error("socks5 no acceptable auth method"));
        buf = buf.subarray(2);
        if (method === 2) {                                 // RFC 1929
          const u = Buffer.from(proxy.username ?? ""), p = Buffer.from(proxy.password ?? "");
          sock.write(Buffer.concat([Buffer.from([1, u.length]), u, Buffer.from([p.length]), p]));
          stage = 1;
          buf = Buffer.alloc(0);
        } else sendConnect();
        return;
      }

      if (stage === 1) {                                    // auth reply
        if (buf.length < 2) return;
        if (buf[1] !== 0) return fail(new Error("socks5 auth rejected"));
        buf = Buffer.alloc(0);
        sendConnect();
        return;
      }

      if (buf.length < 4) return;                           // connect reply
      const rep = buf[1], atyp = buf[3];
      const addrLen = atyp === 1 ? 4 : atyp === 4 ? 16 : 1 + buf[4];
      const need = 4 + addrLen + 2;
      if (buf.length < need) return;
      if (rep !== 0) return fail(new Error(`socks5 connect failed (rep=${rep})`));
      sock.setTimeout(0);
      const rest = buf.subarray(need);
      if (rest.length) sock.unshift(rest);
      settled = true;
      resolve(sock);
    });
  });
}

// SOCKS4a CONNECT: DSTIP 0.0.0.1 means "resolve the hostname that follows", so DNS
// happens at the proxy like socks5's ATYP=3. No auth is defined by SOCKS4.
function socks4Tunnel(proxy, host, port, timeout = TIMEOUT) {
  return new Promise((resolve, reject) => {
    const sock = withTimeout(net.connect({ host: proxy.hostname, port: Number(proxy.port || 1080) }), "socks4", timeout);
    let buf = Buffer.alloc(0);
    let settled = false;
    const fail = (e) => { if (settled) return; settled = true; sock.destroy(); reject(e); };

    sock.on("error", () => {});   // a late reset after settle must not become an uncaughtException
    sock.once("error", fail);
    sock.once("close", () => fail(new Error("socks4 connection closed during handshake")));
    sock.once("connect", () => {
      const h = Buffer.from(host);
      sock.write(Buffer.concat([
        Buffer.from([4, 1, (port >> 8) & 0xff, port & 0xff, 0, 0, 0, 1, 0]),   // cmd, port, 0.0.0.1, empty userid
        h, Buffer.from([0]),
      ]));
    });
    sock.on("data", (chunk) => {
      buf = Buffer.concat([buf, chunk]);
      if (buf.length < 8) return;
      if (buf[0] !== 0) return fail(new Error(`socks4 bad version ${buf[0]}`));
      if (buf[1] !== 0x5a) return fail(new Error(`socks4 connect failed (code=${buf[1]})`));
      sock.setTimeout(0);
      const rest = buf.subarray(8);
      if (rest.length) sock.unshift(rest);
      settled = true;
      resolve(sock);
    });
  });
}

async function tlsOver(raw, host, timeout = TIMEOUT) {
  const sock = tls.connect({ socket: raw, servername: host });
  sock.setTimeout(timeout, () => sock.destroy(new Error("tls timeout")));
  sock.on("error", () => {});   // handshake failures are still surfaced below; later ones are per-request
  await new Promise((resolve, reject) => {
    sock.once("secureConnect", resolve);
    sock.once("error", reject);
    sock.once("close", () => reject(new Error("tls closed during handshake")));
  });
  return sock;
}

const dial = async (proxy, host, port = 443, timeout = TIMEOUT) => {
  if (!proxy) return null;
  const raw = isSocks4(proxy) ? await socks4Tunnel(proxy, host, port, timeout)
    : isSocks(proxy) ? await socks5Tunnel(proxy, host, port, timeout)
      : await httpTunnel(proxy, host, port, timeout);
  // A free proxy can RST between tunnel setup and tls.connect attaching its listeners;
  // without a handler that surfaces as an uncaughtException and takes down the whole batch.
  raw.on("error", () => {});
  return tlsOver(raw, host, timeout);
};

async function get(url, { referer = PAGE, proxy = null, timeout = TIMEOUT } = {}) {
  const u = new URL(url);
  const headers = { "User-Agent": UA, Accept: "text/html", ...(referer ? { Referer: referer } : {}) };
  let mod = https;
  let options = { hostname: u.hostname, path: u.pathname + u.search, headers, timeout };

  const sock = await dial(proxy, u.hostname, 443, timeout);
  if (sock) {
    mod = http;   // socket is already TLS; speak plain HTTP over it
    options = {
      createConnection: () => sock,
      host: u.hostname, port: 443, path: u.pathname + u.search, setHost: false,
      // Connection: close is load-bearing — a pooled tunnel socket gets reused for a later
      // request through a dead proxy, and the run hangs on an unsettled promise
      headers: { Host: u.hostname, Connection: "close", ...headers }, timeout,
    };
  }

  return new Promise((resolve, reject) => {
    const req = mod.request(options, (res) => {
      if (res.statusCode >= 400) { res.resume(); return reject(new Error(`${res.statusCode} ${url}`)); }
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    });
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.on("error", reject);
    req.end();
  });
}

/* ------------------------------------------------------------------- tor ctrl */

const portOpen = (host, port) => new Promise((resolve) => {
  const s = net.connect({ host, port });
  const done = (ok) => { s.destroy(); resolve(ok); };
  s.setTimeout(400, () => done(false));
  s.once("connect", () => done(true));
  s.once("error", () => done(false));
});

async function firstOpen(envVal, candidates) {
  if (envVal) return Number(envVal);
  for (const p of candidates) if (await portOpen("127.0.0.1", p)) return p;
  return null;
}

const COOKIE_PATHS = [
  process.env.TOR_COOKIE,
  `${process.env.USERPROFILE ?? ""}/Desktop/Tor Browser/Browser/TorBrowser/Data/Tor/control_auth_cookie`,
  `${process.env.APPDATA ?? ""}/Tor Browser/Browser/TorBrowser/Data/Tor/control_auth_cookie`,
  "/var/run/tor/control.authcookie",
  "/var/lib/tor/control_auth_cookie",
].filter(Boolean);

async function torSetup() {
  const socksPort = await firstOpen(process.env.TOR_SOCKS_PORT, [9050, 9150]);
  if (!socksPort) throw new Error("no Tor SOCKS port on 9050/9150 — is Tor running? set TOR_SOCKS_PORT");
  const socks = new URL(`socks5://127.0.0.1:${socksPort}`);

  const ctrlPort = await firstOpen(process.env.TOR_CONTROL_PORT, [9051, 9151]);
  let control = null;
  if (ctrlPort) {
    try {
      control = await torControl(ctrlPort);
    } catch (e) {
      console.error(`Tor control port ${ctrlPort} unusable (${e.message}); votes will reuse circuits — exits repeat, so expect duplicates.`);
    }
  } else {
    console.error("no Tor control port on 9051/9151; cannot rotate circuits, expect duplicate exits.");
  }
  return { socks, control };
}

async function torControl(port) {
  const sock = net.connect({ host: "127.0.0.1", port });
  await waitConnect(sock);

  let acc = "";
  let pending = null;
  let dead = false;
  // Tor drops the control connection after a failed AUTHENTICATE. Without this the pending
  // reply never settles and the whole process exits 13 on an unsettled top-level await.
  sock.on("close", () => { dead = true; const p = pending; pending = null; p?.("551 control connection closed"); });
  sock.on("error", () => {});
  sock.on("data", (d) => {
    acc += d.toString();
    const lines = acc.split("\r\n");
    if (lines.length < 2) return;
    if (!/^\d{3} /.test(lines[lines.length - 2])) return;   // multi-line reply still open
    const reply = acc;
    acc = "";
    const p = pending;
    pending = null;
    p?.(reply);
  });
  // serialized: concurrent SIGNALs would clobber the single pending reply
  let chain = Promise.resolve();
  const send = (cmd) => (chain = chain.then(() => dead
    ? "551 control connection closed"
    : new Promise((resolve) => { pending = resolve; sock.write(cmd + "\r\n"); })));

  // Every existing cookie is tried, newest first: a stale Tor Browser cookie must not win
  // over the running instance's own.
  const cookies = COOKIE_PATHS.filter(existsSync)
    .map((p) => ({ p, m: statSync(p).mtimeMs }))
    .sort((a, b) => b.m - a.m);
  const attempts = cookies.map((c) => `AUTHENTICATE ${readFileSync(c.p).toString("hex")}`);
  if (process.env.TOR_CONTROL_PASSWORD) attempts.push(`AUTHENTICATE "${process.env.TOR_CONTROL_PASSWORD.replace(/"/g, '\\"')}"`);
  attempts.push('AUTHENTICATE ""');   // CookieAuthentication 0

  let last = "";
  for (const cmd of attempts) {
    last = await send(cmd);
    if (/^250 /.test(last)) break;
  }
  if (!/^250 /.test(last)) throw new Error(`auth failed: ${last.trim()} — set TOR_COOKIE or TOR_CONTROL_PASSWORD`);

  return { newnym: () => send("SIGNAL NEWNYM") };
}

/* ----------------------------------------------------------------- poll logic */

// escape() semantics used by the poll's own vote-js URL builder
const esc = (s) => encodeURIComponent(s).replace(/%2F/g, "/").replace(/%3A/g, ":");

async function resolvePoll(proxy) {
  let pollId = process.env.POLL_ID;

  if (!pollId) {
    let html = null;
    try {
      html = await get(PAGE, { referer: null, proxy });
      pollId = html?.match(/secure\.polldaddy\.com\/p\/(\d+)\.js/)?.[1];
    } catch (e) {
      // If direct connection 403s on Cloudflare (common on cloud servers), route through Tor
      if (tor && !proxy) {
        try {
          const torProxy = streams ? isolated(tor.socks.port) : tor.socks;
          html = await get(PAGE, { referer: null, proxy: torProxy });
          pollId = html?.match(/secure\.polldaddy\.com\/p\/(\d+)\.js/)?.[1];
        } catch (_) {}
      }
    }
  }

  // Active Bigg Boss Tamil poll ID fallback if the blog wrapper was blocked by Cloudflare
  if (!pollId) {
    pollId = "11073604";
  }

  const js = await get(`https://secure.polldaddy.com/p/${pollId}.js`, { proxy });
  const hash = js.match(new RegExp(`PDV_h${pollId} = '([^']+)'`))?.[1];
  const multiple = js.match(new RegExp(`PDV_pt${pollId} = (\\d+)`))?.[1] ?? "0";
  const va = js.match(new RegExp(`PDV_va${pollId} = (\\d+)`))?.[1] ?? "0";

  const answers = {};
  const re = new RegExp(`PDV_A${pollId}\\[\\d+\\]\\[0\\] = '(\\d+)'[\\s\\S]{0,80}?\\[1\\] = '([^']*)'`, "g");
  for (const m of js.matchAll(re)) answers[m[2]] = m[1];
  if (!hash || !Object.keys(answers).length) throw new Error("could not parse poll metadata");

  return { pollId, hash, multiple, va, answers };
}

async function handshake(poll, proxy, timeout = TIMEOUT) {
  const hs = await get(`https://poll.fm/n/${poll.hash}/${poll.pollId}`, { proxy, timeout });
  const nonce = hs.match(new RegExp(`PDV_n${poll.pollId}='([^']+)'`))?.[1];
  if (!nonce) throw new Error(`no nonce in handshake response: ${hs.slice(0, 120)}`);
  return nonce;
}

async function vote(poll, answerId, proxy, nonce = null) {
  // nonce is short-lived: fetch and spend it in the same breath
  const n = nonce ?? (await handshake(poll, proxy));

  const url = `https://polls.polldaddy.com/vote-js.php?p=${poll.pollId}&b=${poll.multiple}` +
    `&a=${answerId},&o=&va=${poll.va}&cookie=0&tags=${poll.pollId}-src:poll-embed` +
    `&n=${esc(n)}&url=${esc(PAGE)}`;
  let res = await get(url, { proxy });

  // Every submit is answered with an arithmetic challenge first; the vote only registers when
  // it is re-sent with maths=1 and that challenge's key. The old code treated the challenge
  // form as a counted vote, so it reported success without ever casting one.
  const q = res.match(/>\s*(\d+)\s*([+\-*x])\s*(\d+)\s*=\s*</);
  const mathsKey = res.match(/name="maths_key" value="([^"]+)"/)?.[1];
  if (q && mathsKey) {
    const a = Number(q[1]), b = Number(q[3]);
    const answer = q[2] === "+" ? a + b : q[2] === "-" ? a - b : a * b;
    res = await get(`${url}&maths=1&answer=${answer}&maths_key=${mathsKey}`, { proxy });
  }

  if (/alert\("(.*?)"\)/.test(res)) throw new Error(res.match(/alert\("(.*?)"\)/)[1]);
  const already = /already counted|already-registered/i.test(res);
  const results = /pds-feedback-per/.test(res) && !/pds-maths-form/.test(res);
  if (!already && !results) throw new Error(`unrecognised vote response: ${res.slice(0, 120)}`);
  const fresh = !already;
  const tally = [...res.matchAll(/title="([^"]+)">[\s\S]*?pds-feedback-per">[^0-9]*([\d.]+)%[\s\S]*?\(([\d,]+) votes\)/g)]
    .map(m => ({ name: m[1], pct: m[2], votes: m[3] }));
  return { fresh, tally };
}

// The address the poll sees for this circuit. Same SOCKS username = same circuit = same exit,
// so probing first tells us whether the vote would be thrown away as "already counted".
async function exitIp(proxy, timeout = 8000) {
  const probes = [
    ["https://api.ipify.org?format=json", /"ip"\s*:\s*"([^"]+)"/],
    ["https://icanhazip.com/", /^\s*([0-9a-f:.]+)\s*$/im],
    ["https://www.cloudflare.com/cdn-cgi/trace", /(?:^|\n)ip=([0-9a-f:.]+)/im],
  ];
  for (const [url, re] of probes) {
    try {
      const m = (await get(url, { proxy, timeout })).match(re);
      if (m) return m[1];
    } catch {}
  }
  return null;
}

/* -------------------------------------------------------------- proxy sources */

// ProxyScrape's free-proxy page renders this API's payload (the page links to it directly):
// https://proxyscrape.com/free-proxy-list -> https://api.proxyscrape.com/v4/free-proxy-list/get
const PROXYSCRAPE_API = "https://api.proxyscrape.com/v4/free-proxy-list/get" +
  "?request=display_proxies&proxy_format=protocolipport&format=text&timeout=10000";

async function fetchProxyscrape(protocols) {
  const found = [];
  const failures = [];
  await Promise.all(protocols.map(async (proto) => {
    try {
      const text = await get(`${PROXYSCRAPE_API}&protocol=${encodeURIComponent(proto)}`,
        { referer: null, timeout: 30000 });
      const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
      if (!lines.length) throw new Error("empty response");
      found.push(...lines.map(normalize));
    } catch (e) {
      failures.push(`${proto}: ${e.message}`);
    }
  }));
  if (!found.length) throw new Error(`no proxies returned (${failures.join("; ")})`);
  if (failures.length) console.error(`proxyscrape: ${failures.join("; ")}`);
  return found;
}

// Keep one proxy per host: the poll credits an IP, not a port, and free lists repeat hosts.
function dedupeProxies(list) {
  const seen = new Set();
  return list.filter((p) => {
    const k = key(p);
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

// Probe every proxy with a real HTTPS GET. A proxy that cannot CONNECT/TLS is useless here,
// and the probe doubles as an egress-IP census so two proxies on one IP cost one vote, not two.
async function validateProxyList(list, lanes, probe = null) {
  const valid = [];
  const exits = new Set();
  const byProto = new Map();
  let cursor = 0, probed = 0, dead = 0, spentHits = 0, pollDead = 0;
  const total = list.length;
  const startedAt = Date.now();
  const breakdown = () => [...byProto].map(([p, n]) => `${p}:${n}`).join(" ") || "none";
  const report = () => process.stderr.write(`\r  validated ${probed}/${total}: ${valid.length} usable ` +
    `(${breakdown()}) (${dead} dead/dup, ${pollDead} poll-blocked, ${spentHits} spent) ` +
    `${((Date.now() - startedAt) / 1000).toFixed(0)}s`);

  const lane = async () => {
    while (cursor < list.length) {
      const proxy = list[cursor++];
      const ip = await exitIp(proxy, validateTimeout);
      probed++;
      if (!ip || exits.has(ip)) {
        dead++;
      } else if (isTorSpent(ip)) {
        spentHits++;
      } else {
        exits.add(ip);
        // A proxy that tunnels ipify can still MITM or block the poll host; spend one cheap
        // handshake now so `vote` does not discover that after an attempt.
        let reachable = true;
        if (probe) {
          try { await probe(proxy); } catch { reachable = false; pollDead++; }
        }
        if (reachable) {
          proxyExit.set(key(proxy), ip);
          valid.push(proxy);
          const proto = proxy.protocol.replace(":", "");
          byProto.set(proto, (byProto.get(proto) ?? 0) + 1);
        }
      }
      if (probed % 100 === 0) report();
    }
  };
  await Promise.all(Array.from({ length: Math.min(lanes, Math.max(1, list.length)) }, lane));
  report();
  process.stderr.write("\n");
  return valid;
}

if (proxySrc === "auto") {
  console.error("--proxies auto was removed; use --proxies proxyscrape or a file path.");
  process.exit(1);
}
if (proxySrc && proxySrc !== "proxyscrape" && !existsSync(proxySrc)) {
  console.error(`proxy list not found: ${proxySrc}`);
  process.exit(1);
}
let staticProxies = proxySrc && proxySrc !== "proxyscrape"
  ? dedupeProxies(readFileSync(proxySrc, "utf8").split(/\s+/).filter(Boolean).map(normalize))
  : [];
const proxyExit = new Map();            // proxy host key -> egress IP seen during validation
const tor = torMode || streams ? await torSetup() : null;
let roundRobin = 0;
let lastTorAt = 0;
const proxyFailures = new Map();
const deadProxies = new Set();

// Ledger of IPs the server already credited, so later runs do not re-spend attempts on them.
// Empty until the poll id (and so the default filename) is known.
let spent = new Set();
let ledgerPath = "";
const WINDOW_MS = 3 * 60 * 60 * 1000;   // the poll re-credits an IP after ~3h
const spentAt = new Map();              // Tor exit ip -> when it last voted
const inFlight = new Set();             // exit ips currently claimed by a lane
const isTorSpent = (ip) => {
  const t = spentAt.get(ip);
  if (t == null) return false;
  if (Date.now() - t < WINDOW_MS) return true;
  spentAt.delete(ip);                   // window rolled; the exit is usable again
  return false;
};
const markTorSpent = (ip) => {
  if (!ip || isTorSpent(ip)) return;
  const now = Date.now();
  spentAt.set(ip, now);
  appendFileSync(ledgerPath, `${ip}\t${now}\n`);
};
const markSpent = (p) => {
  // a loopback proxy (Tor's SOCKS, ssh -D) votes from an exit IP that is not its own
  if (/^(?:127\.|localhost$|\[?::1\]?$)/.test(p.hostname)) return;
  const k = key(p);
  if (spent.has(k)) return;
  spent.add(k);
  appendFileSync(ledgerPath, k + "\n");
};

// An unused SOCKS username means an unused isolation key, so Tor builds a fresh circuit and
// the vote leaves from a different exit. That removes both the NEWNYM throttle and the control port.
const isolated = (port) => new URL(`socks5://s${Math.random().toString(36).slice(2, 10)}:x@127.0.0.1:${port}`);

async function nextProxy() {
  if (tor && !staticProxies.length) return streams ? isolated(tor.socks.port) : tor.socks;
  for (let n = 0; n < staticProxies.length; n++) {
    const p = staticProxies[roundRobin++ % staticProxies.length];
    if (!spent.has(key(p)) && !deadProxies.has(key(p))) return p;
  }
  if (staticProxies.length) return undefined;   // list exhausted, nothing to fall back to
  if (tor) {
    const wait = 11000 - (Date.now() - lastTorAt);
    if (wait > 0) await new Promise((r) => setTimeout(r, wait));
    lastTorAt = Date.now();
    return tor.socks;
  }
  return envProxy ? normalize(envProxy) : null;
}

/* ----------------------------------------------------------------------- main */

let poll = null;
for (let attempt = 1; attempt <= 5; attempt++) {
  try {
    poll = await resolvePoll(null);
    if (poll) break;
  } catch (err) {
    console.error(`Poll resolve attempt ${attempt}/5: ${err.message}. Retrying via proxy...`);
    const p = tor ? (streams ? isolated(tor.socks.port) : tor.socks) : null;
    try {
      poll = await resolvePoll(p);
      if (poll) break;
    } catch (_) {}
    await new Promise((r) => setTimeout(r, 2000));
  }
}
if (!poll) {
  console.error("Fatal: Failed to resolve poll metadata after 5 attempts.");
  process.exit(1);
}
const normName = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
const ballotKeys = Object.keys(poll.answers);
const missing = [];
const ballots = names.map((requested) => {
  const wanted = normName(requested);
  const exact = ballotKeys.find((k) => normName(k) === wanted);
  const partial = ballotKeys.find((k) => {
    const n = normName(k);
    return n.includes(wanted) || wanted.includes(n);
  });
  const canonical = exact ?? partial;
  if (!canonical) {
    missing.push(requested);
    return null;
  }
  return {
    requested,
    name: canonical,
    answerId: poll.answers[canonical],
    quota: 0,
    counted: 0,
    dup: 0,
    failed: 0,
  };
});
if (missing.length) {
  console.error(`${missing.map((n) => `"${n}"`).join(", ")} not on the ballot. Options: ${ballotKeys.join(", ")}`);
  process.exit(1);
}
const seenAnswer = new Map();
for (const ballot of ballots) {
  const prior = seenAnswer.get(ballot.answerId);
  if (prior) {
    console.error(`"${prior.requested}" and "${ballot.requested}" both resolve to "${ballot.name}"; use distinct names.`);
    process.exit(1);
  }
  seenAnswer.set(ballot.answerId, ballot);
}
const totalTarget = Number.isFinite(target) && target > 0 ? Math.floor(target) : 0;
for (let i = 0; i < ballots.length; i++) {
  ballots[i].quota = totalTarget > 0
    ? Math.floor(totalTarget / ballots.length) + (i < totalTarget % ballots.length ? 1 : 0)
    : Infinity;
}
ledgerPath = ledgerFlag ?? `spent-${poll.pollId}.txt`;
spent = new Set();
if (existsSync(ledgerPath)) {
  for (const line of readFileSync(ledgerPath, "utf8").split(/\r?\n/)) {
    const [ip, ts] = line.trim().split(/\s+/);
    if (!ip) continue;
    if (ts) {
      if (Date.now() - Number(ts) < WINDOW_MS) spentAt.set(ip, Number(ts));
    } else spent.add(ip);               // legacy proxy line: no timestamp
  }
}

if (proxySrc === "proxyscrape") {
  if (!proxyProtocols.length) {
    console.error(`no valid --proxy-protocols; choose from ${[...VALID_PROTOCOLS].join(",")}`);
    process.exit(1);
  }
  let scraped;
  try {
    scraped = await fetchProxyscrape(proxyProtocols);
  } catch (e) {
    console.error(`proxyscrape fetch failed: ${e.message}`);
    process.exit(1);
  }
  const unique = dedupeProxies(scraped);
  const unspent = unique.filter((p) => !spent.has(key(p)));
  const candidates = maxProxies ? unspent.slice(0, maxProxies) : unspent;
  console.error(`proxyscrape: ${scraped.length} entries, ${unique.length} unique hosts, ` +
    `${unspent.length} unspent${candidates.length < unspent.length ? `, probing first ${candidates.length}` : ""} ` +
    `(${proxyProtocols.join("/")})${validateProxies ? `; probing @${validateConcurrency}` : ""}`);
  staticProxies = validateProxies
    ? await validateProxyList(candidates, validateConcurrency, (p) => handshake(poll, p, validateTimeout))
    : candidates;
  if (!staticProxies.length) {
    console.error("proxyscrape: no usable proxies; free lists rotate, retry or use --tor.");
    process.exit(1);
  }
}
const channels = [
  tor ? `tor 127.0.0.1:${tor.socks.port}${tor.control ? "+newnym" : ""}` : null,
  staticProxies.length
    ? `${staticProxies.length} proxies${proxySrc === "proxyscrape" ? " (proxyscrape)" : ""}`
    : envProxy ? "env proxy" : null,
].filter(Boolean);
const targetPlan = totalTarget > 0
  ? `${totalTarget} total -> ${ballots.map((b) => b.quota).join("/")}`
  : "none";
console.log(`poll ${poll.pollId} | ${ballots.map((b) => `"${b.name}" -> ${b.answerId}`).join(" | ")} | ` +
  `${channels.join(" + ") || "direct"} | plan ${times} attempts @${concurrency} | target ${targetPlan} | ` +
  `ledger ${spent.size + spentAt.size} spent${dryRun ? " | DRY RUN" : ""}`);

if (dryRun) {
  // prove the nonce handshake works without spending a vote
  const proxy = tor ? tor.socks : null;
  const hs = await get(`https://poll.fm/n/${poll.hash}/${poll.pollId}`, { proxy });
  console.log(`handshake ok${proxy ? " via tor" : ""}:`, hs.match(new RegExp(`PDV_n${poll.pollId}='([^']+)'`))?.[1]);
  process.exit(0);
}

if (times > 1 && !staticProxies.length && !envProxy && !tor) {
  console.error(`no proxy source: all ${times} attempts go out from this IP, so only the first one counts.`);
}

const stats = { counted: 0, dup: 0, skipped: 0, failed: 0, startedAt: Date.now() };
const maxSkips = Math.max(100, times * 20);
let tallyShown = false;
let next = 0;
const nameInFlight = new Array(ballots.length).fill(0);
const finished = () => next >= times || ballots.every((b) => b.counted >= b.quota);

function pickBallot() {
  let best = -1;
  let bestDeficit = 0;
  let bestUsed = Infinity;
  for (let i = 0; i < ballots.length; i++) {
    const ballot = ballots[i];
    const used = ballot.counted + nameInFlight[i];
    if (used >= ballot.quota) continue;
    if (Number.isFinite(ballot.quota)) {
      const deficit = ballot.quota - used;
      if (deficit > bestDeficit) { best = i; bestDeficit = deficit; }
    } else if (used < bestUsed) {
      best = i;
      bestUsed = used;
    }
  }
  if (best < 0) return null;
  nameInFlight[best]++;
  return { index: best, ballot: ballots[best] };
}

async function worker() {
  while (!finished()) {
    const i = next++;
    let proxy;
    try {
      proxy = await nextProxy();
    } catch (e) {
      stats.failed++;
      console.error(`[${i + 1}/${times}] proxy source failed: ${e.message}`);
      continue;
    }
    if (proxy === undefined) {
      console.error("proxy source exhausted; stopping");
      times = 0;
      break;
    }

    const label = proxy
      ? (proxy === tor?.socks ? "tor" : tor && proxy.hostname === "127.0.0.1" ? "tor-stream" : proxy.host)
      : "direct";
    // Resolve the exit before spending a vote request. The handshake is needed anyway, and
    // running it first warms the fresh circuit so the echo probe is not the one paying for
    // Tor's 3-5s circuit build (8 cold lanes at once were timing out).
    const local = Boolean(proxy && /^(?:127\.|localhost$|\[?::1\]?$)/.test(proxy.hostname));
    // Validation already told us the egress IP for free proxies; Tor's is per-circuit.
    let exit = proxy ? proxyExit.get(key(proxy)) ?? null : null;
    let nonce = null;
    if (local) {
      // rotate first: the handshake, probe and vote must all use the same circuit
      if (tor?.control && proxy === tor.socks) {
        try { await tor.control.newnym(); } catch (e) { console.error(`[${i + 1}/${times}] newnym failed: ${e.message}`); }
      }
      try { nonce = await handshake(poll, proxy); } catch (e) { console.error(`[${i + 1}/${times}] handshake failed via ${label}: ${e.message}`); }
      exit = await exitIp(proxy).catch(() => null);
      if (!exit) console.error(`[${i + 1}/${times}] exit ip unknown via ${label}, voting blind`);
    }
    if (exit && (isTorSpent(exit) || inFlight.has(exit))) {
      stats.skipped++;
      console.error(`[${i + 1}/${times}] skip exit ${exit} (${isTorSpent(exit) ? "spent" : "busy"}) via ${label}`);
      // A static list just advances to the next proxy; Tor without a control port cannot change circuits at all.
      if (!staticProxies.length && !streams && !tor?.control) {
        console.error("non-stream tor cannot change circuits without a control port; use --tor-streams");
        times = 0;
        break;
      }
      next--;   // a skip spends no vote attempt
      if (stats.skipped >= maxSkips) {
        console.error(`skipped ${stats.skipped} spent exits; the pool is exhausted for ~3h`);
        times = 0;
      }
      continue;
    }
    const pick = pickBallot();
    if (!pick) break;
    const nameIndex = pick.index;
    const ballot = pick.ballot;
    if (exit) inFlight.add(exit);
    try {
      const { fresh, tally } = await vote(poll, ballot.answerId, proxy, nonce);
      if (fresh) {
        stats.counted++;
        ballot.counted++;
      } else {
        stats.dup++;
        ballot.dup++;
      }
      // either way this IP is now locked out of the poll for ~3h
      if (exit) {
        markTorSpent(exit);
        // stop the round-robin from handing this proxy out again: its exit is spent even
        // though the host is not, and a second attempt could only be skipped
        if (proxy && proxy !== tor?.socks) spent.add(key(proxy));
      } else if (proxy && proxy !== tor?.socks) markSpent(proxy);
      const quota = Number.isFinite(ballot.quota) ? `/${ballot.quota}` : "";
      console.log(`[${i + 1}/${times}] ${fresh ? "vote counted" : "already counted"} for ${ballot.name} via ${label} ` +
        `(${ballot.counted}${quota} for ${ballot.name}, ${stats.counted} total)`);
      const mine = tally.find((t) => t.name === ballot.name);
      if (tallyEach && mine) console.log(`    tally: ${mine.name} ${mine.votes}`);
      if (!tallyShown && tally.length) {
        tallyShown = true;
        console.log(tally.map(t => `  ${t.name.padEnd(16)} ${t.pct.padStart(6)}%  ${t.votes}`).join("\n"));
      }
    } catch (e) {
      stats.failed++;
      ballot.failed++;
      console.error(`[${i + 1}/${times}] failed for ${ballot.name} via ${label}: ${e.message}`);
      if (staticProxies.length && proxy && proxy !== tor?.socks) {
        const k = key(proxy);
        const fails = (proxyFailures.get(k) ?? 0) + 1;
        proxyFailures.set(k, fails);
        // CONNECT 400 is a dead endpoint; auth/sync/network errors get a few retries.
        const limit = /CONNECT 400\b/.test(e.message) ? 1 : 3;
        if (fails >= limit && !deadProxies.has(k)) {
          deadProxies.add(k);
          console.error(`[${i + 1}/${times}] dropping ${proxy.host} after ${fails} failure(s)`);
        }
      }
    } finally {
      nameInFlight[nameIndex] = Math.max(0, nameInFlight[nameIndex] - 1);
      if (exit) inFlight.delete(exit);
    }
    if (delay) await new Promise(r => setTimeout(r, delay));
  }
}

await Promise.all(Array.from({ length: Math.min(concurrency, Math.max(times, concurrency)) }, worker));
const secs = (Date.now() - stats.startedAt) / 1000;
const attempts = stats.counted + stats.dup + stats.failed;
console.log(`\ncounted ${stats.counted} | already-counted ${stats.dup} | skipped ${stats.skipped} | failed ${stats.failed}`);
console.log(`win rate ${(100 * stats.counted / Math.max(1, attempts)).toFixed(1)}% of ${attempts} attempts | ` +
  `${secs.toFixed(0)}s | ${(stats.counted / secs * 60).toFixed(1)} votes/min | ` +
  `ledger ${spent.size + spentAt.size} spent`);
console.log("\nper name:");
for (const ballot of ballots) {
  const quota = Number.isFinite(ballot.quota) ? `/${ballot.quota}` : "";
  console.log(`  ${ballot.name.padEnd(16)} ${String(ballot.counted).padStart(4)}${quota} counted | ` +
    `already-counted ${ballot.dup} | failed ${ballot.failed}`);
}
process.exit(0);
