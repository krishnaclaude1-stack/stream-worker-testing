import http from "node:http";
import { spawn } from "node:child_process";
import net from "node:net";

const PORT = Number(process.env.PORT || 3000);
const DEFAULT_CONTESTANTS = ["Lakshmi Priya", "Jason", "Muniyammal"];
const CONTESTANTS = String(process.env.VOTE_CONTESTANTS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
if (!CONTESTANTS.length) CONTESTANTS.push(...DEFAULT_CONTESTANTS);
const CONTESTANT = CONTESTANTS.join(", ");
const TARGET_NAMES = CONTESTANTS.map((n) => n.toLowerCase());
const isTargetName = (name) => {
  const n = name.toLowerCase();
  return TARGET_NAMES.some((t) => n.includes(t) || t.includes(n));
};
const TOR_STREAMS = process.env.TOR_STREAMS || "12";
const BATCH_TIMES = process.env.BATCH_TIMES || "2000";
const BATCH_TARGET = process.env.BATCH_TARGET || "500";
const BATCH_DELAY = process.env.BATCH_DELAY || "0";
const CYCLE_COOLDOWN = Math.max(1, Number(process.env.CYCLE_COOLDOWN || "2"));
const TOR_SOCKS_PORT = Number(process.env.TOR_SOCKS_PORT || "9050");

// Live aggregated state
const state = {
  status: "initializing",
  startTime: Date.now(),
  contestant: CONTESTANT,
  torStreams: TOR_STREAMS,
  batchTarget: Number(BATCH_TARGET),
  batchTimes: Number(BATCH_TIMES),
  currentCycle: 1,
  cycleStartTime: Date.now(),
  batchCounted: 0,
  totalCounted: 0,
  totalDuplicates: 0,
  totalFailed: 0,
  lastVoteTime: null,
  pollStandings: [],
  logs: [],
  torOnline: false,
};

function log(msg, type = "info") {
  const time = new Date().toLocaleTimeString();
  const entry = { time, msg, type };
  state.logs.push(entry);
  if (state.logs.length > 200) state.logs.shift();
  console.log(`[${time}] ${msg}`);
}

// Calculate metrics
function getMetrics() {
  const uptimeSecs = Math.max(1, Math.floor((Date.now() - state.startTime) / 1000));
  const totalAttempts = state.totalCounted + state.totalDuplicates + state.totalFailed;
  const winRate = totalAttempts > 0 ? ((state.totalCounted / totalAttempts) * 100).toFixed(1) : "0.0";
  const votesPerMin = ((state.totalCounted / uptimeSecs) * 60).toFixed(1);

  return {
    ...state,
    uptimeSecs,
    totalAttempts,
    winRate,
    votesPerMin,
  };
}

// Tor port checker
function checkTor(port = TOR_SOCKS_PORT) {
  return new Promise((resolve) => {
    const s = net.connect({ host: "127.0.0.1", port });
    s.setTimeout(1500, () => { s.destroy(); resolve(false); });
    s.once("connect", () => { s.destroy(); resolve(true); });
    s.once("error", () => { s.destroy(); resolve(false); });
  });
}

// Parsing script logs to extract live metrics
function parseOutputLine(line) {
  const trimmed = line.trim();
  if (!trimmed) return;

  // Log to ring buffer
  let logType = "info";
  if (/vote counted/i.test(trimmed)) logType = "success";
  else if (/already counted/i.test(trimmed)) logType = "warn";
  else if (/failed/i.test(trimmed) || /error/i.test(trimmed)) logType = "error";

  log(trimmed, logType);

  // Check for successful vote
  const countedMatch = trimmed.match(/\[(\d+)\/(\d+)\]\s+vote counted/i);
  if (countedMatch) {
    const total = trimmed.match(/,\s*(\d+)\s+total\)\s*$/i)?.[1]
      ?? trimmed.match(/\((\d+)\s+counted\)\s*$/i)?.[1];
    state.batchCounted = total ? Number(total) : state.batchCounted + 1;
    state.totalCounted++;
    state.lastVoteTime = new Date().toLocaleTimeString();
    return;
  }

  // Duplicate / already counted
  if (/already counted.*via/i.test(trimmed)) {
    state.totalDuplicates++;
    return;
  }

  // Failed vote
  if (/failed.*via/i.test(trimmed)) {
    state.totalFailed++;
    return;
  }

  // Candidate tally single line: "tally: Lakshmi 12,345"
  const singleTally = trimmed.match(/tally:\s+([A-Za-z0-9 .'-]+?)\s+([\d,]+)/i);
  if (singleTally) {
    const name = singleTally[1].trim();
    const votes = singleTally[2].trim();
    const existing = state.pollStandings.find((s) => s.name.toLowerCase() === name.toLowerCase());
    if (existing) {
      existing.votes = votes;
    } else {
      state.pollStandings.push({ name, votes, pct: "—", isTarget: isTargetName(name) });
    }
    return;
  }

  // Table row: "  Lakshmi          34.5%  14,210"
  const tableMatch = trimmed.match(/^([A-Za-z0-9 .'-]+?)\s{2,}([\d.]+)%\s+([\d,]+)/);
  if (tableMatch) {
    const name = tableMatch[1].trim();
    const pct = tableMatch[2].trim();
    const votes = tableMatch[3].trim();
    const isTarget = isTargetName(name);

    const idx = state.pollStandings.findIndex((s) => s.name.toLowerCase() === name.toLowerCase());
    const item = { name, pct, votes, isTarget };
    if (idx >= 0) {
      state.pollStandings[idx] = item;
    } else {
      state.pollStandings.push(item);
    }
  }
}

// Supervisor loop for bbb-vote-old.mjs
let currentChild = null;
let isShuttingDown = false;

async function runVotingSupervisor() {
  log("Waiting for Tor proxy before starting voting loop...");
  state.status = "waiting_for_tor";

  let torReady = false;
  for (let i = 0; i < 45; i++) {
    if (await checkTor()) {
      torReady = true;
      break;
    }
    await new Promise((r) => setTimeout(r, 1000));
  }

  state.torOnline = torReady;
  if (!torReady) {
    log("Warning: Tor SOCKS port 9050 not reachable yet. Voting will proceed attempting connection.", "warn");
  } else {
    log("Tor SOCKS proxy confirmed ready on 127.0.0.1:" + TOR_SOCKS_PORT, "success");
  }

  while (!isShuttingDown) {
    state.status = "voting";
    state.batchCounted = 0;
    state.cycleStartTime = Date.now();
    log(`>>> Starting Batch #${state.currentCycle} for "${CONTESTANT}" (Target: ${BATCH_TARGET}) <<<`, "info");

    const args = [
      "worker.mjs",
      ...CONTESTANTS,
      "--tor-streams", TOR_STREAMS,
      "--times", BATCH_TIMES,
      "--target", BATCH_TARGET,
      "--tally-each",
    ];

    if (BATCH_DELAY && Number(BATCH_DELAY) > 0) {
      args.push("--delay", BATCH_DELAY);
    }

    try {
      await new Promise((resolve) => {
        currentChild = spawn("node", args, {
          env: { ...process.env, TOR_SOCKS_PORT: String(TOR_SOCKS_PORT), POLL_TIMEOUT: process.env.POLL_TIMEOUT || "45000" },
          stdio: ["ignore", "pipe", "pipe"],
        });

        currentChild.stdout.on("data", (chunk) => {
          const lines = chunk.toString().split(/\r?\n/);
          for (const l of lines) parseOutputLine(l);
        });

        currentChild.stderr.on("data", (chunk) => {
          const lines = chunk.toString().split(/\r?\n/);
          for (const l of lines) parseOutputLine(l);
        });

        currentChild.on("close", (code) => {
          log(`Batch #${state.currentCycle} finished with exit code ${code}`, code === 0 ? "info" : "warn");
          currentChild = null;
          resolve();
        });

        currentChild.on("error", (err) => {
          log(`Batch runner error: ${err.message}`, "error");
          currentChild = null;
          resolve();
        });
      });
    } catch (e) {
      log(`Unexpected batch error: ${e.message}`, "error");
    }

    if (isShuttingDown) break;

    state.currentCycle++;
    state.status = "cooldown";
    log(`Batch completed. Restarting next batch in ${CYCLE_COOLDOWN}s...`, "info");
    await new Promise((r) => setTimeout(r, CYCLE_COOLDOWN * 1000));
  }
}

// Clean shutdown
function shutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  log("Shutting down Railway 24/7 supervisor...", "warn");
  if (currentChild) {
    try { currentChild.kill("SIGTERM"); } catch (_) {}
  }
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

// Web Dashboard HTML generator
function renderDashboard() {
  const m = getMetrics();
  const uptimeMinutes = Math.floor(m.uptimeSecs / 60);
  const uptimeHours = Math.floor(uptimeMinutes / 60);
  const uptimeDays = Math.floor(uptimeHours / 24);
  const formattedUptime = `${uptimeDays}d ${uptimeHours % 24}h ${uptimeMinutes % 60}m ${m.uptimeSecs % 60}s`;

  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bigg Boss Tamil Voting Engine — 24/7 Railway Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-main: #090d16;
      --card-bg: rgba(17, 24, 39, 0.75);
      --card-border: rgba(255, 255, 255, 0.08);
      --card-hover: rgba(255, 255, 255, 0.12);
      --accent-emerald: #10b981;
      --accent-glow: rgba(16, 185, 129, 0.25);
      --accent-cyan: #06b6d4;
      --accent-indigo: #6366f1;
      --text-primary: #f9fafb;
      --text-secondary: #94a3b8;
      --text-muted: #64748b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: var(--bg-main);
      background-image: 
        radial-gradient(circle at 15% 15%, rgba(16, 185, 129, 0.07) 0%, transparent 45%),
        radial-gradient(circle at 85% 85%, rgba(99, 102, 241, 0.07) 0%, transparent 45%);
      color: var(--text-primary);
      font-family: 'Outfit', sans-serif;
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      padding: 1.5rem;
    }
    .container { max-width: 1200px; margin: 0 auto; width: 100%; }

    /* Header */
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 1rem;
      margin-bottom: 2rem;
      padding-bottom: 1.25rem;
      border-bottom: 1px solid var(--card-border);
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 0.85rem;
    }
    .brand-icon {
      width: 44px;
      height: 44px;
      border-radius: 12px;
      background: linear-gradient(135deg, #10b981, #06b6d4);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.35rem;
      box-shadow: 0 4px 20px var(--accent-glow);
    }
    .brand-title {
      font-size: 1.35rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }
    .brand-subtitle {
      font-size: 0.82rem;
      color: var(--text-secondary);
      display: flex;
      align-items: center;
      gap: 0.4rem;
    }
    .pulse-badge {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.45rem 0.9rem;
      border-radius: 9999px;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #34d399;
      font-size: 0.82rem;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #10b981;
      box-shadow: 0 0 10px #10b981;
      animation: pulse 1.8s infinite ease-in-out;
    }
    @keyframes pulse {
      0%, 100% { transform: scale(1); opacity: 1; }
      50% { transform: scale(1.4); opacity: 0.5; }
    }

    /* Grid layout */
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 1.25rem;
      margin-bottom: 1.5rem;
    }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      backdrop-filter: blur(12px);
      border-radius: 16px;
      padding: 1.25rem 1.4rem;
      transition: all 0.25s ease;
      position: relative;
      overflow: hidden;
    }
    .card:hover {
      border-color: var(--card-hover);
      transform: translateY(-2px);
    }
    .card-label {
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-secondary);
      font-weight: 600;
      margin-bottom: 0.4rem;
    }
    .card-value {
      font-size: 2.1rem;
      font-weight: 800;
      letter-spacing: -0.03em;
      display: flex;
      align-items: baseline;
      gap: 0.4rem;
    }
    .card-subtext {
      font-size: 0.8rem;
      color: var(--text-muted);
      margin-top: 0.35rem;
    }
    .value-emerald { color: #34d399; text-shadow: 0 0 24px rgba(16, 185, 129, 0.3); }
    .value-cyan { color: #38bdf8; }
    .value-indigo { color: #a5b4fc; }
    .value-amber { color: #fbbf24; }

    /* Standings & Progress Section */
    .row-2col {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.25rem;
      margin-bottom: 1.5rem;
    }
    @media (max-width: 860px) {
      .row-2col { grid-template-columns: 1fr; }
    }

    .section-title {
      font-size: 1.05rem;
      font-weight: 700;
      margin-bottom: 1rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    /* Leaderboard */
    .standings-list {
      display: flex;
      flex-direction: column;
      gap: 0.75rem;
    }
    .standing-item {
      display: flex;
      flex-direction: column;
      gap: 0.4rem;
      padding: 0.8rem 1rem;
      border-radius: 12px;
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid rgba(255, 255, 255, 0.04);
      transition: all 0.2s ease;
    }
    .standing-item.target-candidate {
      background: rgba(16, 185, 129, 0.08);
      border-color: rgba(16, 185, 129, 0.3);
      box-shadow: 0 0 15px rgba(16, 185, 129, 0.1);
    }
    .standing-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .standing-name {
      font-weight: 600;
      font-size: 0.95rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .target-tag {
      font-size: 0.65rem;
      padding: 0.15rem 0.45rem;
      border-radius: 4px;
      background: #10b981;
      color: #042f2c;
      font-weight: 700;
    }
    .standing-stats {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.88rem;
      font-weight: 600;
    }
    .bar-bg {
      height: 6px;
      width: 100%;
      background: rgba(255, 255, 255, 0.06);
      border-radius: 9999px;
      overflow: hidden;
    }
    .bar-fill {
      height: 100%;
      border-radius: 9999px;
      background: linear-gradient(90deg, #10b981, #06b6d4);
      transition: width 0.5s ease;
    }
    .bar-fill.normal {
      background: rgba(255, 255, 255, 0.2);
    }

    /* Terminal Console */
    .terminal-card {
      background: #060a12;
      border: 1px solid var(--card-border);
      border-radius: 16px;
      overflow: hidden;
      margin-bottom: 1.5rem;
    }
    .terminal-header {
      background: #0b111e;
      padding: 0.75rem 1.25rem;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--card-border);
    }
    .terminal-dots {
      display: flex;
      gap: 6px;
    }
    .terminal-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
    }
    .terminal-dot.red { background: #ef4444; }
    .terminal-dot.yellow { background: #f59e0b; }
    .terminal-dot.green { background: #10b981; }
    .terminal-title {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
      color: var(--text-secondary);
    }
    .terminal-body {
      height: 290px;
      overflow-y: auto;
      padding: 1rem 1.25rem;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.8rem;
      line-height: 1.6;
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }
    .log-line {
      display: flex;
      gap: 0.75rem;
      word-break: break-word;
    }
    .log-time { color: var(--text-muted); flex-shrink: 0; }
    .log-msg { color: #e2e8f0; }
    .log-success { color: #34d399; }
    .log-warn { color: #fbbf24; }
    .log-error { color: #f87171; }

    /* Footer */
    footer {
      margin-top: auto;
      padding-top: 1.5rem;
      border-top: 1px solid var(--card-border);
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: var(--text-muted);
      font-size: 0.8rem;
      flex-wrap: wrap;
      gap: 0.5rem;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <div class="brand-icon">⚡</div>
        <div>
          <h1 class="brand-title">Bigg Boss Tamil Vote Engine</h1>
          <div class="brand-subtitle">
            <span>24/7 Automation on Railway</span>
            <span>•</span>
            <span id="uptime-text">Uptime: ${formattedUptime}</span>
          </div>
        </div>
      </div>
      <div class="pulse-badge">
        <span class="pulse-dot"></span>
        <span id="status-badge">RUNNING 24/7 (CYCLE #${m.currentCycle})</span>
      </div>
    </header>

    <!-- Top Key Metrics -->
    <div class="grid">
      <div class="card">
        <div class="card-label">Total Votes Counted</div>
        <div class="card-value value-emerald" id="stat-total-counted">+${m.totalCounted.toLocaleString()}</div>
        <div class="card-subtext" id="stat-batch-progress">${m.batchCounted} / ${m.batchTarget} in current batch</div>
      </div>

      <div class="card">
        <div class="card-label">Target Contestant</div>
        <div class="card-value value-cyan" id="stat-contestant">${m.contestant}</div>
        <div class="card-subtext" id="stat-streams">${m.torStreams} Parallel Isolated Tor Streams</div>
      </div>

      <div class="card">
        <div class="card-label">Success / Win Rate</div>
        <div class="card-value value-indigo" id="stat-win-rate">${m.winRate}%</div>
        <div class="card-subtext" id="stat-attempts">${m.totalAttempts} total IP attempts</div>
      </div>

      <div class="card">
        <div class="card-label">Voting Velocity</div>
        <div class="card-value value-amber" id="stat-velocity">${m.votesPerMin} <span style="font-size: 1rem; color: var(--text-secondary);">/min</span></div>
        <div class="card-subtext" id="stat-last-vote">Last counted: ${m.lastVoteTime || 'Booting...'}</div>
      </div>
    </div>

    <!-- 2 Column Section: Batch Details & Live Standings -->
    <div class="row-2col">
      <!-- Current Batch Status -->
      <div class="card">
        <div class="section-title">
          <span>Current Batch Progress</span>
          <span style="font-size: 0.85rem; color: var(--text-secondary);" id="batch-badge">Batch #${m.currentCycle}</span>
        </div>
        <div style="margin-bottom: 1.25rem;">
          <div style="display: flex; justify-content: space-between; font-size: 0.85rem; margin-bottom: 0.5rem;">
            <span style="color: var(--text-secondary);">Target: ${m.batchTarget} votes</span>
            <span style="font-weight: 700; color: #34d399;" id="batch-pct">${((m.batchCounted / Math.max(1, m.batchTarget)) * 100).toFixed(0)}%</span>
          </div>
          <div class="bar-bg" style="height: 10px;">
            <div class="bar-fill" id="batch-bar" style="width: ${Math.min(100, (m.batchCounted / Math.max(1, m.batchTarget)) * 100)}%;"></div>
          </div>
        </div>
        <div style="display: flex; justify-content: space-between; font-size: 0.82rem; color: var(--text-secondary); border-top: 1px solid var(--card-border); padding-top: 0.8rem;">
          <span>Duplicates skipped: <strong id="stat-dup" style="color: #f1f5f9;">${m.totalDuplicates}</strong></span>
          <span>Failures: <strong id="stat-fail" style="color: #f1f5f9;">${m.totalFailed}</strong></span>
          <span>Tor Port: <strong style="color: #34d399;">9050</strong></span>
        </div>
      </div>

      <!-- Live Poll Standings -->
      <div class="card">
        <div class="section-title">
          <span>Live Poll Standings</span>
          <span style="font-size: 0.75rem; color: #10b981;">AUTO-UPDATED</span>
        </div>
        <div class="standings-list" id="standings-container">
          ${renderStandingsHtml(m.pollStandings, m.contestant)}
        </div>
      </div>
    </div>

    <!-- Live Terminal Activity Feed -->
    <div class="terminal-card">
      <div class="terminal-header">
        <div class="terminal-dots">
          <div class="terminal-dot red"></div>
          <div class="terminal-dot yellow"></div>
          <div class="terminal-dot green"></div>
        </div>
        <div class="terminal-title">live-stream :: tor-worker.log</div>
        <div style="font-size: 0.75rem; color: var(--text-muted);">auto-scroll: ON</div>
      </div>
      <div class="terminal-body" id="terminal-feed">
        ${renderLogsHtml(m.logs)}
      </div>
    </div>

    <footer>
      <div>Bigg Boss Tamil 24/7 Automated Voting Engine • Deployed on Railway</div>
      <div>Railway Container: Healthy 🟢 • Node.js ${process.version}</div>
    </footer>
  </div>

  <script>
    // Real-time AJAX refresh every 2.5 seconds
    async function updateStats() {
      try {
        const res = await fetch('/api/stats');
        if (!res.ok) return;
        const d = await res.json();

        document.getElementById('stat-total-counted').textContent = '+' + d.totalCounted.toLocaleString();
        document.getElementById('stat-batch-progress').textContent = d.batchCounted + ' / ' + d.batchTarget + ' in current batch';
        document.getElementById('stat-contestant').textContent = d.contestant;
        document.getElementById('stat-win-rate').textContent = d.winRate + '%';
        document.getElementById('stat-attempts').textContent = d.totalAttempts + ' total IP attempts';
        document.getElementById('stat-velocity').innerHTML = d.votesPerMin + ' <span style="font-size: 1rem; color: var(--text-secondary);">/min</span>';
        if (d.lastVoteTime) document.getElementById('stat-last-vote').textContent = 'Last counted: ' + d.lastVoteTime;
        
        document.getElementById('status-badge').textContent = (d.status === 'voting' ? 'RUNNING 24/7' : d.status.toUpperCase()) + ' (CYCLE #' + d.currentCycle + ')';
        document.getElementById('batch-badge').textContent = 'Batch #' + d.currentCycle;
        
        const pct = Math.min(100, Math.round((d.batchCounted / Math.max(1, d.batchTarget)) * 100));
        document.getElementById('batch-pct').textContent = pct + '%';
        document.getElementById('batch-bar').style.width = pct + '%';
        
        document.getElementById('stat-dup').textContent = d.totalDuplicates;
        document.getElementById('stat-fail').textContent = d.totalFailed;

        // Update Uptime
        const upM = Math.floor(d.uptimeSecs / 60);
        const upH = Math.floor(upM / 60);
        const upD = Math.floor(upH / 24);
        document.getElementById('uptime-text').textContent = 'Uptime: ' + upD + 'd ' + (upH % 24) + 'h ' + (upM % 60) + 'm ' + (d.uptimeSecs % 60) + 's';

        // Update Standings
        if (d.pollStandings && d.pollStandings.length > 0) {
          let html = '';
          for (const s of d.pollStandings) {
            const isTarget = s.isTarget;
            html += '<div class="standing-item ' + (isTarget ? 'target-candidate' : '') + '">';
            html += '  <div class="standing-row">';
            html += '    <div class="standing-name">' + s.name + (isTarget ? ' <span class="target-tag">TARGET</span>' : '') + '</div>';
            html += '    <div class="standing-stats">' + (s.pct !== '—' ? s.pct + '% • ' : '') + s.votes + ' votes</div>';
            html += '  </div>';
            html += '  <div class="bar-bg">';
            html += '    <div class="bar-fill ' + (isTarget ? '' : 'normal') + '" style="width: ' + (parseFloat(s.pct) || 0) + '%;"></div>';
            html += '  </div>';
            html += '</div>';
          }
          document.getElementById('standings-container').innerHTML = html;
        }

        // Update Terminal Logs
        if (d.logs && d.logs.length > 0) {
          const feed = document.getElementById('terminal-feed');
          let logHtml = '';
          for (const l of d.logs) {
            const cls = l.type === 'success' ? 'log-success' : l.type === 'warn' ? 'log-warn' : l.type === 'error' ? 'log-error' : 'log-msg';
            logHtml += '<div class="log-line"><span class="log-time">[' + l.time + ']</span><span class="' + cls + '">' + escapeHtml(l.msg) + '</span></div>';
          }
          feed.innerHTML = logHtml;
          feed.scrollTop = feed.scrollHeight;
        }
      } catch (err) {
        console.error('Stats poll error', err);
      }
    }

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    setInterval(updateStats, 2500);
  </script>
</body>
</html>`;
}

function renderStandingsHtml(standings, target) {
  if (!standings || standings.length === 0) {
    return `<div style="color: var(--text-muted); font-size: 0.85rem; padding: 1rem; text-align: center;">Standings will populate after first vote handshake...</div>`;
  }
  return standings.map((s) => {
    const isTarget = s.isTarget;
    return `
      <div class="standing-item ${isTarget ? 'target-candidate' : ''}">
        <div class="standing-row">
          <div class="standing-name">
            ${s.name} ${isTarget ? '<span class="target-tag">TARGET</span>' : ''}
          </div>
          <div class="standing-stats">${s.pct !== '—' ? s.pct + '% • ' : ''}${s.votes} votes</div>
        </div>
        <div class="bar-bg">
          <div class="bar-fill ${isTarget ? '' : 'normal'}" style="width: ${parseFloat(s.pct) || 0}%;"></div>
        </div>
      </div>
    `;
  }).join('');
}

function renderLogsHtml(logs) {
  if (!logs || logs.length === 0) {
    return `<div class="log-line"><span class="log-msg">Waiting for worker output...</span></div>`;
  }
  return logs.slice(-60).map((l) => {
    const cls = l.type === "success" ? "log-success" : l.type === "warn" ? "log-warn" : l.type === "error" ? "log-error" : "log-msg";
    return `<div class="log-line"><span class="log-time">[${l.time}]</span><span class="${cls}">${l.msg.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")}</span></div>`;
  }).join('');
}

// HTTP Server
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health" || url.pathname === "/api/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify({ status: "ok", uptime: Math.floor((Date.now() - state.startTime) / 1000) }));
  }

  if (url.pathname === "/api/stats") {
    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    return res.end(JSON.stringify(getMetrics()));
  }

  if (url.pathname === "/") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    return res.end(renderDashboard());
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("Not Found");
});

server.listen(PORT, "0.0.0.0", () => {
  log(`Web Dashboard & Healthcheck active at http://0.0.0.0:${PORT}`, "success");
  // Launch the 24/7 background voting supervisor
  runVotingSupervisor().catch((err) => {
    log(`Fatal supervisor error: ${err.message}`, "error");
  });
});
