'use strict';

// ── State ──────────────────────────────────────────────────────────────────
const sparklines = {};   // label → Float64Array ring buffer
const SPARK_LEN  = 60;
let   schemas    = {};

// ── Boot ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  startClock();
  loadSchemas();
  startSSE();
  document.getElementById('explain-btn').addEventListener('click', runExplain);
});

// ── Clock ──────────────────────────────────────────────────────────────────
function startClock() {
  const el = document.getElementById('clock');
  const tick = () => {
    el.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });
  };
  tick();
  setInterval(tick, 1000);
}

// ── SSE stream ─────────────────────────────────────────────────────────────
function startSSE() {
  const dot = document.getElementById('live-dot');
  const es  = new EventSource('/api/events');

  es.onmessage = (e) => {
    dot.classList.remove('offline');
    const { snapshot, graph } = JSON.parse(e.data);
    renderGraph(graph, snapshot);
    renderStageCards(snapshot.Stages || []);
  };

  es.onerror = () => {
    dot.classList.add('offline');
    es.close();
    setTimeout(startSSE, 3000);
  };
}

// ── Pipeline graph — schematic strip ──────────────────────────────────────
const NODE_R  = 7;
const CY      = 26;   // vertical centre of 52px canvas
const MIN_STEP = 120;
const SIDE_PAD = 50;

function renderGraph(graph, snapshot) {
  const svg = document.getElementById('graph-canvas');
  const W   = svg.clientWidth || 640;
  const n   = graph.length;
  if (!n) { svg.innerHTML = ''; return; }

  const step  = n > 1 ? Math.max(MIN_STEP, (W - SIDE_PAD * 2) / (n - 1)) : 0;
  const startX = n > 1 ? Math.max(SIDE_PAD, (W - step * (n - 1)) / 2) : W / 2;

  const stageMap = {};
  (snapshot.Stages || []).forEach(s => { stageMap[s.Label] = s; });

  const maxTp = Math.max(1, ...(snapshot.Stages || []).map(s => s.Throughput || 0));

  let html = `<defs>
    <marker id="g-arr" markerWidth="5" markerHeight="4"
            refX="4" refY="2" orient="auto">
      <polygon points="0 0,5 2,0 4" style="fill:var(--rule)"/>
    </marker>
  </defs>`;

  const xLast = startX + (n - 1) * step;

  // Horizontal rail
  html += `<line class="g-rail"
    x1="${startX - NODE_R}" y1="${CY}"
    x2="${xLast + NODE_R + 6}" y2="${CY}"
    marker-end="url(#g-arr)"/>`;

  graph.forEach((node, i) => {
    const x  = startX + i * step;
    const st = stageMap[node.Label] || {};
    const h  = nodeHealth(st);
    const tp = formatThroughputShort(st.Throughput);

    // Animated signal dot for this edge
    if (i < n - 1 && (st.Throughput || 0) > 0) {
      const x2    = startX + (i + 1) * step;
      const dur   = (Math.max(0.35, 2.5 - (st.Throughput / maxTp) * 2.1)).toFixed(2);
      const delay = (i * 0.15).toFixed(2);
      html += `<circle class="g-dot" r="3">
        <animateMotion dur="${dur}s" begin="${delay}s"
          repeatCount="indefinite"
          path="M${x + NODE_R},${CY} L${x2 - NODE_R},${CY}"/>
      </circle>`;
    }

    // Node: filled circle + coloured ring
    html += `<circle class="g-node-bg" cx="${x}" cy="${CY}" r="${NODE_R}"/>`;
    html += `<circle class="g-node-ring ${h}" cx="${x}" cy="${CY}" r="${NODE_R}"/>`;

    // Stage label — above rail
    html += `<text class="g-label" x="${x}" y="${CY - NODE_R - 5}">${node.Label}</text>`;

    // Throughput — below rail
    if (st.Throughput !== undefined) {
      html += `<text class="g-tput" x="${x}" y="${CY + NODE_R + 11}">${tp}/s</text>`;
    }
  });

  svg.innerHTML = html;
}

function nodeHealth(st) {
  if (!st || st.Throughput === undefined) return 'idle';
  if (st.ErrorTotal  >   0) return 'error';
  if (st.QueueDepth  > 200) return 'warn';
  return 'ok';
}

// ── Stage cards ────────────────────────────────────────────────────────────
function renderStageCards(stages) {
  const grid = document.getElementById('stages-grid');

  stages.forEach(st => {
    const id   = 'card-' + st.Label.replace(/[^a-z0-9]/gi, '-');
    let   card = document.getElementById(id);

    if (!card) {
      card = document.createElement('div');
      card.id = id;
      card.className = 'stage-card';
      card.innerHTML = `
        <p class="card-name">${st.Label}</p>
        <div class="card-throughput">
          <span class="throughput-value" data-f="tp">0</span>
          <span class="throughput-unit">/s</span>
        </div>
        <div class="sparkline-wrap">
          <svg class="sparkline" viewBox="0 0 120 22" preserveAspectRatio="none">
            <polygon class="sparkline-area" points=""/>
            <polyline class="sparkline-line" points=""/>
          </svg>
        </div>
        <div class="mini-metrics">
          <div class="mini-metric">
            <span class="mini-label">p50</span>
            <span class="mini-value" data-f="p50">—</span>
          </div>
          <div class="mini-metric">
            <span class="mini-label">p99</span>
            <span class="mini-value" data-f="p99">—</span>
          </div>
          <div class="mini-metric">
            <span class="mini-label">queue</span>
            <span class="mini-value" data-f="queue">—</span>
          </div>
        </div>`;
      grid.appendChild(card);
      sparklines[st.Label] = new Float64Array(SPARK_LEN);
    }

    card.className = `stage-card health-${nodeHealth(st)}`;

    card.querySelector('[data-f=tp]').textContent    = formatThroughputShort(st.Throughput);
    card.querySelector('[data-f=p50]').textContent   = formatDuration(st.LatencyP50);
    card.querySelector('[data-f=p99]').textContent   = formatDuration(st.LatencyP99);
    card.querySelector('[data-f=queue]').textContent = st.QueueDepth ?? 0;

    const p99el = card.querySelector('[data-f=p99]');
    p99el.className = 'mini-value' + (st.LatencyP99 > 200e6 ? ' warn' : '');

    const buf = sparklines[st.Label];
    buf.copyWithin(0, 1);
    buf[SPARK_LEN - 1] = st.Throughput || 0;
    drawSparkline(card.querySelector('.sparkline'), buf);
  });
}

function drawSparkline(svgEl, buf) {
  const W = 120, H = 22;
  const max = Math.max(...buf, 1);
  const pts = Array.from(buf).map((v, i) => {
    const x = (i / (SPARK_LEN - 1)) * W;
    const y = H - (v / max) * (H - 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
  const area = `0,${H} ${pts} ${W},${H}`;
  svgEl.querySelector('.sparkline-line').setAttribute('points', pts);
  svgEl.querySelector('.sparkline-area').setAttribute('points', area);
}

// ── Schema timeline ────────────────────────────────────────────────────────
async function loadSchemas() {
  const refresh = async () => {
    try {
      const r = await fetch('/api/schemas');
      schemas = await r.json();
      renderSchemas();
    } catch (e) {
      console.warn('schema load failed', e);
    }
  };
  await refresh();
  setInterval(refresh, 5000);
}

function renderSchemas() {
  const list = document.getElementById('schema-list');
  const ids  = Object.keys(schemas);

  if (!ids.length) {
    list.innerHTML = '<span class="placeholder-text">No schemas registered.</span>';
    return;
  }

  let html = '';
  for (const id of ids) {
    const versions = schemas[id];
    const sorted = [...versions].sort((a, b) => a.Version - b.Version);
    html += `<div class="schema-block">
      <p class="schema-id-label">${id}</p>
      <div class="schema-versions">`;

    let prevNames = new Set();
    sorted.forEach((v, i) => {
      const isLatest = i === sorted.length - 1;
      const cur = new Set(v.Fields.map(f => f.Name));
      html += `<div class="schema-version ${isLatest ? 'is-latest' : ''}">
        <span class="sv-badge">v${v.Version}</span>
        <div class="sv-fields">`;
      v.Fields.forEach(f => {
        const isNew = prevNames.size > 0 && !prevNames.has(f.Name);
        html += `<span class="sv-field ${isNew ? 'is-new' : ''}">${f.Name}</span>`;
      });
      html += `</div></div>`;
      prevNames = cur;
    });

    html += `</div></div>`;
  }
  list.innerHTML = html;
}

// ── AI explain ─────────────────────────────────────────────────────────────
async function runExplain() {
  const btn = document.getElementById('explain-btn');
  const out = document.getElementById('ai-output');

  btn.disabled    = true;
  btn.textContent = 'analysing…';
  out.innerHTML   = '<span class="placeholder-text">Asking Claude to diagnose your pipeline…</span>';

  try {
    const res = await fetch('/api/explain');
    if (!res.ok) {
      out.textContent = `Error: ${await res.text()}`;
      return;
    }
    out.innerHTML = markdownToHtml(await res.text());
  } catch (e) {
    out.textContent = `Network error: ${e.message}`;
  } finally {
    btn.disabled    = false;
    btn.textContent = 'analyse pipeline';
  }
}

function markdownToHtml(md) {
  return md
    .replace(/```[\s\S]*?```/g, m =>
      `<pre>${escHtml(m.slice(3, -3).replace(/^[a-z]*\n/, ''))}</pre>`)
    .replace(/`([^`]+)`/g, (_, c) => `<code>${escHtml(c)}</code>`)
    .replace(/^## (.+)$/gm, (_, t) => `<h2>${t}</h2>`)
    .replace(/^[-*] (.+)$/gm, (_, t) => `<div>· ${t}</div>`)
    .replace(/^\d+\. (.+)$/gm, (_, t) => `<div style="padding-left:10px">${t}</div>`)
    .replace(/\n{2,}/g, '<br><br>')
    .replace(/\n/g, '<br>');
}

function escHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── Formatters ─────────────────────────────────────────────────────────────
function formatThroughputShort(rps) {
  if (!rps || rps < 1) return '0';
  if (rps >= 1e6) return `${(rps / 1e6).toFixed(1)}M`;
  if (rps >= 1e3) return `${(rps / 1e3).toFixed(1)}k`;
  return Math.round(rps).toString();
}

function formatThroughput(rps) {
  if (!rps || rps < 1) return '0/s';
  if (rps >= 1e6) return `${(rps / 1e6).toFixed(1)}M/s`;
  if (rps >= 1e3) return `${(rps / 1e3).toFixed(1)}k/s`;
  return `${Math.round(rps)}/s`;
}

// Duration arrives as nanoseconds (Go time.Duration serialised as int64)
function formatDuration(ns) {
  if (!ns) return '—';
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(1)}s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(1)}ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(0)}µs`;
  return `${ns}ns`;
}
