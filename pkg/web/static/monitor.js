'use strict';

// Monitor — the live dashboard: SSE-driven graph, stage cards, schema timeline,
// AI explain, and an advanced per-stage metrics drawer. Hardened with reconnect
// backoff and idle/empty states.
const Monitor = (() => {
  const SPARK_LEN = 60;
  const sparklines = {};        // label → Float64Array ring buffer
  let schemas = {};
  let es = null;
  let sseGen = 0;               // guards against stale reconnects after job switch
  let backoff = 1000;           // SSE reconnect backoff (ms), capped
  let currentJob = '';          // which job the monitor is watching ('' = sole/none)
  let lastSnapshot = null;
  let lastGraph = [];
  let drawerLabel = null;       // open stage drawer, or null
  let booted = false;

  function init() {
    if (booted) return;
    booted = true;
    loadSchemas();
    startSSE();
    document.getElementById('explain-btn').addEventListener('click', runExplain);
    document.getElementById('drawer-close').addEventListener('click', closeDrawer);
    document.getElementById('drawer-scrim').addEventListener('click', closeDrawer);
  }

  // setJob points the monitor at a specific job and restarts the SSE stream.
  function setJob(name) {
    if (name === currentJob && es) return;
    currentJob = name || '';
    if (es) es.close();
    resetCards();
    startSSE();
  }

  function resetCards() {
    const grid = document.getElementById('stages-grid');
    grid.innerHTML = '';
    for (const k in sparklines) delete sparklines[k];
    if (drawerLabel) closeDrawer();
  }

  // ── SSE with capped exponential backoff ───────────────────────────────────
  function startSSE() {
    const gen = ++sseGen;
    const dot = document.getElementById('live-dot');
    es = new EventSource(Drift.eventsURL(currentJob));

    es.onopen = () => { backoff = 1000; window.UI.reconnecting(false); };

    es.onmessage = (e) => {
      dot.classList.remove('offline');
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      if (msg.state === 'idle' || !msg.snapshot) {
        renderIdle();
        return;
      }
      showRunning();
      lastSnapshot = msg.snapshot;
      lastGraph = msg.graph || [];
      renderGraph(msg.graph || [], msg.snapshot);
      renderStageCards(msg.snapshot.Stages || []);
      if (drawerLabel) renderDrawer();
    };

    es.onerror = () => {
      if (gen !== sseGen) return; // superseded by a job switch
      dot.classList.add('offline');
      window.UI.reconnecting(true);
      es.close();
      const delay = backoff;
      setTimeout(() => { if (gen === sseGen) startSSE(); }, delay);
      backoff = Math.min(backoff * 2, 15000) + Math.random() * 400; // jitter
    };
  }

  function showRunning() {
    document.getElementById('monitor-empty').hidden = true;
    document.getElementById('stages-grid').hidden = false;
  }

  function renderIdle() {
    lastSnapshot = null;
    const grid = document.getElementById('stages-grid');
    grid.innerHTML = '';
    grid.hidden = true;
    for (const k in sparklines) delete sparklines[k];
    document.getElementById('monitor-empty').hidden = false;
    renderGraph([], { Stages: [] });
    if (drawerLabel) closeDrawer();
  }

  // ── Pipeline graph — schematic strip ──────────────────────────────────────
  const NODE_R = 7, CY = 26, MIN_STEP = 120, SIDE_PAD = 50;

  function renderGraph(graph, snapshot) {
    const svg = document.getElementById('graph-canvas');
    const W = svg.clientWidth || 640;
    const n = graph.length;
    if (!n) { svg.innerHTML = ''; return; }

    const step = n > 1 ? Math.max(MIN_STEP, (W - SIDE_PAD * 2) / (n - 1)) : 0;
    const startX = n > 1 ? Math.max(SIDE_PAD, (W - step * (n - 1)) / 2) : W / 2;
    const stageMap = {};
    (snapshot.Stages || []).forEach((s) => { stageMap[s.Label] = s; });
    const maxTp = Math.max(1, ...(snapshot.Stages || []).map((s) => s.Throughput || 0));

    let html = `<defs><marker id="g-arr" markerWidth="5" markerHeight="4" refX="4" refY="2" orient="auto">
      <polygon points="0 0,5 2,0 4" style="fill:var(--rule)"/></marker></defs>`;
    const xLast = startX + (n - 1) * step;
    html += `<line class="g-rail" x1="${startX - NODE_R}" y1="${CY}" x2="${xLast + NODE_R + 6}" y2="${CY}" marker-end="url(#g-arr)"/>`;

    graph.forEach((node, i) => {
      const x = startX + i * step;
      const st = stageMap[node.Label] || {};
      const h = nodeHealth(st);
      const tp = formatThroughputShort(st.Throughput);
      if (i < n - 1 && (st.Throughput || 0) > 0) {
        const x2 = startX + (i + 1) * step;
        const dur = Math.max(0.35, 2.5 - (st.Throughput / maxTp) * 2.1).toFixed(2);
        html += `<circle class="g-dot" r="3"><animateMotion dur="${dur}s" begin="${(i * 0.15).toFixed(2)}s"
          repeatCount="indefinite" path="M${x + NODE_R},${CY} L${x2 - NODE_R},${CY}"/></circle>`;
      }
      html += `<circle class="g-node-bg" cx="${x}" cy="${CY}" r="${NODE_R}"/>`;
      html += `<circle class="g-node-ring ${h}" cx="${x}" cy="${CY}" r="${NODE_R}"/>`;
      html += `<text class="g-label" x="${x}" y="${CY - NODE_R - 5}">${esc(node.Label)}</text>`;
      if (st.Throughput !== undefined) {
        html += `<text class="g-tput" x="${x}" y="${CY + NODE_R + 11}">${tp}/s</text>`;
      }
    });
    svg.innerHTML = html;
  }

  function nodeHealth(st) {
    if (!st || st.Throughput === undefined) return 'idle';
    if (st.ErrorTotal > 0) return 'error';
    if (st.QueueDepth > 200) return 'warn';
    return 'ok';
  }

  // ── Stage cards ───────────────────────────────────────────────────────────
  function renderStageCards(stages) {
    const grid = document.getElementById('stages-grid');
    const seen = new Set();
    stages.forEach((st) => {
      seen.add(st.Label);
      const id = 'card-' + st.Label.replace(/[^a-z0-9]/gi, '-');
      let card = document.getElementById(id);
      if (!card) {
        card = document.createElement('div');
        card.id = id;
        card.className = 'stage-card';
        card.innerHTML = `
          <p class="card-name">${esc(st.Label)}</p>
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
            <div class="mini-metric"><span class="mini-label">p50</span><span class="mini-value" data-f="p50">—</span></div>
            <div class="mini-metric"><span class="mini-label">p99</span><span class="mini-value" data-f="p99">—</span></div>
            <div class="mini-metric"><span class="mini-label">queue</span><span class="mini-value" data-f="queue">—</span></div>
          </div>`;
        card.addEventListener('click', () => openDrawer(st.Label));
        grid.appendChild(card);
        sparklines[st.Label] = new Float64Array(SPARK_LEN);
      }
      card.className = `stage-card health-${nodeHealth(st)}`;
      card.querySelector('[data-f=tp]').textContent = formatThroughputShort(st.Throughput);
      card.querySelector('[data-f=p50]').textContent = formatDuration(st.LatencyP50);
      card.querySelector('[data-f=p99]').textContent = formatDuration(st.LatencyP99);
      card.querySelector('[data-f=queue]').textContent = st.QueueDepth ?? 0;
      card.querySelector('[data-f=p99]').className = 'mini-value' + (st.LatencyP99 > 200e6 ? ' warn' : '');
      const buf = sparklines[st.Label];
      buf.copyWithin(0, 1);
      buf[SPARK_LEN - 1] = st.Throughput || 0;
      drawSparkline(card.querySelector('.sparkline'), buf);
    });
    // Remove cards for stages that vanished (pipeline swapped).
    Array.from(grid.children).forEach((c) => {
      const label = c.querySelector('.card-name')?.textContent;
      if (label && !seen.has(label)) c.remove();
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
    svgEl.querySelector('.sparkline-line').setAttribute('points', pts);
    svgEl.querySelector('.sparkline-area').setAttribute('points', `0,${H} ${pts} ${W},${H}`);
  }

  // ── Advanced metrics drawer ───────────────────────────────────────────────
  async function openDrawer(label) {
    drawerLabel = label;
    document.getElementById('drawer-scrim').hidden = false;
    document.getElementById('metrics-drawer').hidden = false;
    renderDrawer();
  }

  function closeDrawer() {
    drawerLabel = null;
    document.getElementById('drawer-scrim').hidden = true;
    document.getElementById('metrics-drawer').hidden = true;
  }

  function renderDrawer() {
    if (!lastSnapshot) return;
    const stages = lastSnapshot.Stages || [];
    const st = stages.find((s) => s.Label === drawerLabel);
    document.getElementById('drawer-title').textContent = drawerLabel;
    const body = document.getElementById('drawer-body');
    if (!st) { body.innerHTML = '<span class="placeholder-text">Stage gone.</span>'; return; }

    const totals = stages.reduce((a, s) => {
      a.processed += s.ProcessedTotal || 0;
      a.errors += s.ErrorTotal || 0;
      a.tp += s.Throughput || 0;
      return a;
    }, { processed: 0, errors: 0, tp: 0 });

    const rows = [
      ['throughput', formatThroughput(st.Throughput)],
      ['processed total', (st.ProcessedTotal || 0).toLocaleString()],
      ['errors', (st.ErrorTotal || 0).toString()],
      ['latency p50', formatDuration(st.LatencyP50)],
      ['latency p99', formatDuration(st.LatencyP99)],
      ['queue depth', (st.QueueDepth || 0).toString()],
    ];
    const sel = selectivity(st, stages);
    if (sel) rows.push(['kept', sel]);

    let html = '<div class="metric-grid">';
    rows.forEach(([k, v]) => {
      html += `<div class="metric-cell"><span class="metric-k">${k}</span><span class="metric-v">${v}</span></div>`;
    });
    html += '</div>';

    // Aggregate / output chart + live record tail (filled async from /api/sample).
    html += '<div class="section-head" style="margin-top:18px"><p class="panel-eyebrow">recent output</p>'
      + '<button type="button" class="data-ask" id="data-ask" hidden>✦ ask ai</button></div>';
    html += '<div id="drawer-chart" class="drawer-chart"></div>';
    html += '<div id="drawer-records" class="record-list"><span class="placeholder-text">loading…</span></div>';

    html += '<p class="panel-eyebrow" style="margin-top:18px">pipeline totals</p><div class="metric-grid">';
    html += `<div class="metric-cell"><span class="metric-k">throughput</span><span class="metric-v">${formatThroughput(totals.tp)}</span></div>`;
    html += `<div class="metric-cell"><span class="metric-k">processed</span><span class="metric-v">${totals.processed.toLocaleString()}</span></div>`;
    html += `<div class="metric-cell"><span class="metric-k">errors</span><span class="metric-v">${totals.errors}</span></div>`;
    html += `<div class="metric-cell"><span class="metric-k">uptime</span><span class="metric-v" id="drawer-uptime">…</span></div>`;
    html += '</div>';
    body.innerHTML = html;

    // Attach a "?" to every metric cell (glossary doc + Ask AI with live value).
    body.querySelectorAll('.metric-cell').forEach((cell) => {
      const kEl = cell.querySelector('.metric-k');
      const k = kEl.textContent;
      const v = cell.querySelector('.metric-v').textContent;
      kEl.appendChild(Help.button({
        title: k,
        doc: Help.metricDoc(k),
        subject: `metric "${k}" on stage "${drawerLabel}"`,
        context: `current value: ${v}; stage: ${drawerLabel}`,
      }));
    });

    Drift.get('/api/status').then((s) => {
      const el = document.getElementById('drawer-uptime');
      if (el) el.textContent = s.uptime ? formatDuration(s.uptime) : '—';
    }).catch(() => {});

    Drift.get('/api/sample?job=' + encodeURIComponent(currentJob) + '&stage=' + encodeURIComponent(drawerLabel)).then((res) => {
      if (drawerLabel) renderSample(res.records || []);
    }).catch(() => {});
  }

  // selectivity returns "X% (−N)" for a stage with a single upstream, else null.
  function selectivity(st, stages) {
    const preds = lastGraph.filter((n) => (n.Next || []).includes(st.Label)).map((n) => n.Label);
    if (preds.length !== 1) return null;
    const up = stages.find((s) => s.Label === preds[0]);
    if (!up || !up.ProcessedTotal) return null;
    const kept = (st.ProcessedTotal || 0) / up.ProcessedTotal;
    const dropped = up.ProcessedTotal - (st.ProcessedTotal || 0);
    if (dropped <= 0) return null;
    return `${(kept * 100).toFixed(0)}% (−${dropped.toLocaleString()})`;
  }

  // renderSample draws the live record tail (as a table) + a sparkline of the
  // first numeric field, and wires the "ask ai about this data" button.
  function renderSample(records) {
    const chart = document.getElementById('drawer-chart');
    const list = document.getElementById('drawer-records');
    const askBtn = document.getElementById('data-ask');
    if (!chart || !list) return;
    if (!records.length) {
      chart.innerHTML = '';
      list.innerHTML = '<span class="placeholder-text">no records yet</span>';
      if (askBtn) askBtn.hidden = true;
      return;
    }
    // Sparkline over the first numeric payload field across the sample.
    const field = numericField(records);
    if (field) {
      const vals = records.map((r) => Number(r.Payload[field]) || 0);
      chart.innerHTML = `<div class="chart-label">${esc(field)}: <b>${vals[vals.length - 1]}</b></div>` + miniChart(vals);
    } else {
      chart.innerHTML = '';
    }
    // Newest first, last ~12, as a field table.
    const recent = records.slice(-12).reverse();
    list.innerHTML = recordTable(recent);

    if (askBtn) {
      askBtn.hidden = false;
      askBtn.onclick = () => Help.open(askBtn, {
        title: 'recent data',
        doc: `The last ${recent.length} records emitted by "${drawerLabel}".`,
        subject: `records emitted by stage "${drawerLabel}"`,
        question: 'What does this data look like — any patterns, anomalies, or issues?',
        context: JSON.stringify(recent.slice(0, 6).map((r) => r.Payload)),
      });
    }
  }

  // recordTable renders records as a field table (columns = union of payload keys).
  function recordTable(records) {
    const keys = [];
    records.forEach((r) => Object.keys(r.Payload || {}).forEach((k) => { if (!keys.includes(k)) keys.push(k); }));
    if (!keys.length) return '<span class="placeholder-text">empty records</span>';
    const head = '<tr>' + keys.map((k) => `<th>${esc(k)}</th>`).join('') + '</tr>';
    const rows = records.map((r) => '<tr>' + keys.map((k) => {
      const v = r.Payload[k];
      const s = v === undefined ? '' : (typeof v === 'object' ? JSON.stringify(v) : String(v));
      return `<td title="${esc(s)}">${esc(s)}</td>`;
    }).join('') + '</tr>').join('');
    return `<table class="record-table"><thead>${head}</thead><tbody>${rows}</tbody></table>`;
  }

  function numericField(records) {
    const p = records[records.length - 1].Payload || {};
    for (const k of Object.keys(p)) if (typeof p[k] === 'number') return k;
    return null;
  }

  function miniChart(vals) {
    const W = 280, H = 46, max = Math.max(...vals, 1), min = Math.min(...vals, 0);
    const span = max - min || 1;
    const pts = vals.map((v, i) => {
      const x = vals.length > 1 ? (i / (vals.length - 1)) * W : 0;
      const y = H - 2 - ((v - min) / span) * (H - 6);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    return `<svg class="mini-chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
      <polyline points="${pts}" /></svg>`;
  }

  // ── Schema timeline ───────────────────────────────────────────────────────
  async function loadSchemas() {
    const refresh = async () => {
      try { schemas = await Drift.get('/api/schemas'); renderSchemas(); }
      catch (e) { /* leave previous */ }
    };
    await refresh();
    setInterval(refresh, 5000);
  }

  function renderSchemas() {
    const list = document.getElementById('schema-list');
    const ids = Object.keys(schemas);
    if (!ids.length) { list.innerHTML = '<span class="placeholder-text">No schemas registered.</span>'; return; }
    let html = '';
    for (const id of ids) {
      const sorted = [...schemas[id]].sort((a, b) => a.Version - b.Version);
      html += `<div class="schema-block"><p class="schema-id-label">${esc(id)}</p><div class="schema-versions">`;
      let prev = new Set();
      sorted.forEach((v, i) => {
        const isLatest = i === sorted.length - 1;
        html += `<div class="schema-version ${isLatest ? 'is-latest' : ''}"><span class="sv-badge">v${v.Version}</span><div class="sv-fields">`;
        v.Fields.forEach((f) => {
          const isNew = prev.size > 0 && !prev.has(f.Name);
          html += `<span class="sv-field ${isNew ? 'is-new' : ''}">${esc(f.Name)}</span>`;
        });
        html += `</div></div>`;
        prev = new Set(v.Fields.map((f) => f.Name));
      });
      html += `</div></div>`;
    }
    list.innerHTML = html;
  }

  // ── AI explain ────────────────────────────────────────────────────────────
  async function runExplain() {
    const btn = document.getElementById('explain-btn');
    const out = document.getElementById('ai-output');
    btn.disabled = true; btn.textContent = 'analysing…';
    out.innerHTML = '<span class="placeholder-text">Asking Claude to diagnose your pipeline…</span>';
    try {
      const text = await Drift.get('/api/explain?job=' + encodeURIComponent(currentJob));
      out.innerHTML = markdownToHtml(text);
    } catch (e) {
      out.textContent = `Error: ${e.message}`;
    } finally {
      btn.disabled = false; btn.textContent = 'analyse pipeline';
    }
  }

  function markdownToHtml(md) {
    return md
      .replace(/```[\s\S]*?```/g, (m) => `<pre>${esc(m.slice(3, -3).replace(/^[a-z]*\n/, ''))}</pre>`)
      .replace(/`([^`]+)`/g, (_, c) => `<code>${esc(c)}</code>`)
      .replace(/^## (.+)$/gm, (_, t) => `<h2>${t}</h2>`)
      .replace(/^[-*] (.+)$/gm, (_, t) => `<div>· ${t}</div>`)
      .replace(/^\d+\. (.+)$/gm, (_, t) => `<div style="padding-left:10px">${t}</div>`)
      .replace(/\*\*([^*]+)\*\*/g, (_, t) => `<strong>${t}</strong>`)
      .replace(/\n{2,}/g, '<br><br>').replace(/\n/g, '<br>');
  }

  return { init, setJob, job: () => currentJob };
})();

// ── shared formatters / escaping ────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
function formatThroughputShort(rps) {
  if (!rps || rps < 1) return '0';
  if (rps >= 1e6) return `${(rps / 1e6).toFixed(1)}M`;
  if (rps >= 1e3) return `${(rps / 1e3).toFixed(1)}k`;
  return Math.round(rps).toString();
}
function formatThroughput(rps) {
  if (!rps || rps < 1) return '0/s';
  return formatThroughputShort(rps) + '/s';
}
// Duration arrives as nanoseconds (Go time.Duration serialised as int64).
function formatDuration(ns) {
  if (!ns) return '—';
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(1)}s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(1)}ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(0)}µs`;
  return `${ns}ns`;
}
