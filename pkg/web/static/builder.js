'use strict';

// Builder — visual DAG editor. Drag blocks from the palette, wire operators
// (edges = `next`), configure params, and save/load/run as YAML jobs via the
// control plane. Source→roots and leaves→sink are inferred (drawn dashed), so
// users only wire operator→operator edges. The engine infers the source/sink
// fan-in/out from roots (no incoming) and leaves (no outgoing).
const Builder = (() => {
  const NODE_W = 162, NODE_H = 66, COL_GAP = 220, ROW_GAP = 96;
  let nodes = [];          // {id, kind, type, label, params, x, y, el}
  let edges = [];          // {from, to}  (operator ids)
  let seq = 0;
  let selected = null;     // node id
  let connecting = null;   // node id of pending output port
  let booted = false;

  async function init() {
    if (booted) return;
    booted = true;
    try {
      await Palette.load();
      Palette.renderPalette(document.getElementById('palette-list'));
    } catch (e) {
      document.getElementById('palette-list').innerHTML =
        `<span class="placeholder-text">Palette failed: ${esc(e.message)}</span>`;
    }
    setupCanvasDrop();
    document.getElementById('btn-new').addEventListener('click', () => { if (confirm('Clear the canvas?')) reset(); });
    document.getElementById('btn-save').addEventListener('click', save);
    document.getElementById('btn-validate').addEventListener('click', validate);
    document.getElementById('btn-run').addEventListener('click', run);
    document.getElementById('btn-stop').addEventListener('click', stop);
    document.getElementById('canvas-nodes').addEventListener('click', (e) => {
      if (e.target.id === 'canvas-nodes') deselect();
    });
    refreshJobs();
  }

  // ── canvas drop (palette → node) ──────────────────────────────────────────
  function setupCanvasDrop() {
    const wrap = document.getElementById('canvas-wrap');
    wrap.addEventListener('dragover', (e) => e.preventDefault());
    wrap.addEventListener('drop', (e) => {
      e.preventDefault();
      let payload;
      try { payload = JSON.parse(e.dataTransfer.getData('text/plain')); } catch { return; }
      const rect = wrap.getBoundingClientRect();
      addNode(payload.kind, payload.type, e.clientX - rect.left + wrap.scrollLeft - NODE_W / 2,
        e.clientY - rect.top + wrap.scrollTop - NODE_H / 2);
    });
  }

  // ── model ─────────────────────────────────────────────────────────────────
  function addNode(kind, type, x, y) {
    if (kind === 'source' && nodes.some((n) => n.kind === 'source')) {
      window.UI.toast('Only one source allowed', 'err'); return;
    }
    if (kind === 'sink' && nodes.some((n) => n.kind === 'sink')) {
      window.UI.toast('Only one sink allowed', 'err'); return;
    }
    const block = Palette.find(kind, type);
    if (!block) return;
    const node = {
      id: 'n' + (++seq), kind, type,
      label: kind === 'operator' ? uniqueLabel(type) : kind,
      params: Palette.defaults(block),
      parallelism: 1,
      x: Math.max(0, x), y: Math.max(0, y),
    };
    nodes.push(node);
    renderNode(node);
    drawEdges();
    select(node.id);
    document.getElementById('canvas-hint').hidden = true;
  }

  function uniqueLabel(base) {
    let label = base, i = 1;
    const taken = new Set(nodes.filter((n) => n.kind === 'operator').map((n) => n.label));
    while (taken.has(label)) label = base + '-' + (++i);
    return label;
  }

  function removeNode(id) {
    nodes = nodes.filter((n) => n.id !== id);
    edges = edges.filter((e) => e.from !== id && e.to !== id);
    const el = document.getElementById(id);
    if (el) el.remove();
    if (selected === id) deselect();
    drawEdges();
  }

  // ── node rendering + dragging ─────────────────────────────────────────────
  function renderNode(node) {
    const el = document.createElement('div');
    el.className = 'node kind-' + node.kind;
    el.id = node.id;
    el.style.left = node.x + 'px';
    el.style.top = node.y + 'px';
    el.innerHTML = `
      ${node.kind !== 'source' ? '<span class="port port-in" title="input"></span>' : ''}
      ${node.kind !== 'sink' ? '<span class="port port-out" title="output"></span>' : ''}
      <span class="node-kind">${node.kind}</span>
      <span class="node-title">${esc(node.kind === 'operator' ? node.label : node.type)}</span>
      ${node.kind === 'operator' ? `<span class="node-sub">${esc(node.type)}</span>` : ''}`;
    el.addEventListener('mousedown', (e) => onNodeMouseDown(e, node));
    el.addEventListener('click', (e) => { e.stopPropagation(); select(node.id); });
    const out = el.querySelector('.port-out');
    if (out) out.addEventListener('click', (e) => { e.stopPropagation(); startConnect(node.id); });
    const inp = el.querySelector('.port-in');
    if (inp) inp.addEventListener('click', (e) => { e.stopPropagation(); completeConnect(node.id); });
    document.getElementById('canvas-nodes').appendChild(el);
  }

  function onNodeMouseDown(e, node) {
    if (e.target.classList.contains('port')) return; // ports handle their own clicks
    e.preventDefault();
    const wrap = document.getElementById('canvas-wrap');
    const startX = e.clientX, startY = e.clientY, ox = node.x, oy = node.y;
    let moved = false;
    const move = (ev) => {
      node.x = Math.max(0, ox + (ev.clientX - startX));
      node.y = Math.max(0, oy + (ev.clientY - startY));
      const el = document.getElementById(node.id);
      el.style.left = node.x + 'px'; el.style.top = node.y + 'px';
      moved = true;
      drawEdges();
    };
    const up = () => {
      document.removeEventListener('mousemove', move);
      document.removeEventListener('mouseup', up);
      if (moved) ensureCanvasSize();
    };
    document.addEventListener('mousemove', move);
    document.addEventListener('mouseup', up);
  }

  // ── connections (operator → operator) ─────────────────────────────────────
  function startConnect(id) {
    connecting = id;
    document.getElementById(id)?.classList.add('connecting');
    window.UI.toast('Click an operator/sink input to connect', 'info', 1500);
  }
  function completeConnect(toId) {
    if (!connecting || connecting === toId) { cancelConnect(); return; }
    const from = byId(connecting), to = byId(toId);
    if (to.kind === 'source') { cancelConnect(); return; }
    // op → sink is implicit (leaf→sink); only store op → op edges.
    if (to.kind === 'sink') { window.UI.toast('Sink is wired automatically from leaf blocks', 'info'); cancelConnect(); return; }
    if (from.kind === 'sink') { cancelConnect(); return; }
    if (!edges.some((e) => e.from === connecting && e.to === toId)) {
      edges.push({ from: connecting, to: toId });
    }
    cancelConnect();
    drawEdges();
  }
  function cancelConnect() {
    if (connecting) document.getElementById(connecting)?.classList.remove('connecting');
    connecting = null;
  }

  // ── edge rendering (solid op→op, dashed inferred source/sink) ──────────────
  function drawEdges() {
    const svg = document.getElementById('canvas-edges');
    ensureCanvasSize();
    let html = `<defs><marker id="edge-arr" markerWidth="7" markerHeight="6" refX="6" refY="3" orient="auto">
      <polygon points="0 0,7 3,0 6" fill="var(--ink-3)"/></marker></defs>`;
    const path = (a, b, dashed) => {
      const dx = Math.max(40, Math.abs(b.x - a.x) / 2);
      return `<path class="edge ${dashed ? 'edge-auto' : ''}" d="M${a.x},${a.y} C${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}" marker-end="url(#edge-arr)"/>`;
    };
    edges.forEach((e) => {
      const f = byId(e.from), t = byId(e.to);
      if (f && t) html += path(outPort(f), inPort(t), false);
    });
    // inferred edges
    const src = nodes.find((n) => n.kind === 'source');
    const snk = nodes.find((n) => n.kind === 'sink');
    const ops = nodes.filter((n) => n.kind === 'operator');
    const hasIn = new Set(edges.map((e) => e.to));
    const hasOut = new Set(edges.map((e) => e.from));
    if (src) ops.filter((o) => !hasIn.has(o.id)).forEach((o) => { html += path(outPort(src), inPort(o), true); });
    if (snk) ops.filter((o) => !hasOut.has(o.id)).forEach((o) => { html += path(outPort(o), inPort(snk), true); });
    svg.innerHTML = html;
  }

  const outPort = (n) => ({ x: n.x + NODE_W, y: n.y + NODE_H / 2 });
  const inPort = (n) => ({ x: n.x, y: n.y + NODE_H / 2 });

  function ensureCanvasSize() {
    const svg = document.getElementById('canvas-edges');
    let w = 800, h = 500;
    nodes.forEach((n) => { w = Math.max(w, n.x + NODE_W + 80); h = Math.max(h, n.y + NODE_H + 80); });
    svg.setAttribute('width', w); svg.setAttribute('height', h);
    document.getElementById('canvas-nodes').style.width = w + 'px';
    document.getElementById('canvas-nodes').style.height = h + 'px';
  }

  // ── selection + inspector ─────────────────────────────────────────────────
  function select(id) {
    if (connecting) { completeConnect(id); return; }
    deselectDom();
    selected = id;
    document.getElementById(id)?.classList.add('selected');
    renderInspector();
  }
  function deselect() { selected = null; deselectDom(); renderInspector(); }
  function deselectDom() { document.querySelectorAll('.node.selected').forEach((n) => n.classList.remove('selected')); }

  function renderInspector() {
    const body = document.getElementById('inspector-body');
    body.innerHTML = '';
    if (!selected) { body.innerHTML = '<span class="placeholder-text">Select a block to configure it.</span>'; return; }
    const node = byId(selected);
    const block = Palette.find(node.kind, node.type);

    const head = document.createElement('div');
    head.className = 'insp-head';
    head.innerHTML = `<span class="insp-type">${esc(node.type)}</span><span class="insp-kind">${node.kind}</span>`;
    head.appendChild(Help.button({
      title: node.type,
      doc: block.doc,
      subject: `${node.kind}: ${node.type}`,
      context: 'Configured params: ' + JSON.stringify(node.params),
    }));
    body.appendChild(head);

    if (node.kind === 'operator') {
      const lab = document.createElement('label');
      lab.className = 'param-row';
      lab.innerHTML = '<span class="param-name">label *</span>';
      const inp = document.createElement('input');
      inp.type = 'text'; inp.className = 'param-input'; inp.value = node.label;
      inp.addEventListener('change', () => {
        const v = inp.value.trim();
        if (v && !nodes.some((n) => n.kind === 'operator' && n.id !== node.id && n.label === v)) {
          node.label = v; redrawNodeTitle(node);
        } else { inp.value = node.label; window.UI.toast('Label must be unique and non-empty', 'err'); }
      });
      lab.appendChild(inp);
      body.appendChild(lab);
    }

    if (node.kind === 'operator' && block.parallelizable) {
      const row = document.createElement('label');
      row.className = 'param-row';
      row.innerHTML = '<span class="param-name">parallelism</span>';
      const inp = document.createElement('input');
      inp.type = 'number'; inp.min = '1'; inp.className = 'param-input';
      inp.value = node.parallelism || 1;
      inp.addEventListener('change', () => {
        const n = parseInt(inp.value, 10);
        node.parallelism = n > 1 ? n : 1;
        inp.value = node.parallelism;
      });
      row.appendChild(inp);
      const hint = document.createElement('span');
      hint.className = 'param-hint';
      const keyed = node.type === 'dedup' || node.type === 'session';
      hint.textContent = `Run across N shards on multiple cores (${keyed ? 'partitioned by its key' : 'round-robin'}).`;
      row.appendChild(hint);
      body.appendChild(row);
    }

    body.appendChild(Palette.renderForm(block, node.params, () => {}));

    if (block.doc) {
      const doc = document.createElement('p');
      doc.className = 'insp-doc'; doc.textContent = block.doc;
      body.appendChild(doc);
    }
    const del = document.createElement('button');
    del.type = 'button'; del.className = 'tb-btn insp-delete'; del.textContent = 'delete block';
    del.addEventListener('click', () => removeNode(node.id));
    body.appendChild(del);
  }

  function redrawNodeTitle(node) {
    const el = document.getElementById(node.id);
    if (el) el.querySelector('.node-title').textContent = node.label;
  }

  // ── serialize / deserialize ───────────────────────────────────────────────
  function serialize() {
    const src = nodes.find((n) => n.kind === 'source');
    const snk = nodes.find((n) => n.kind === 'sink');
    const ops = nodes.filter((n) => n.kind === 'operator');
    const labelOf = {}; ops.forEach((o) => (labelOf[o.id] = o.label));
    const nextOf = {}; ops.forEach((o) => (nextOf[o.id] = []));
    edges.forEach((e) => { if (nextOf[e.from]) nextOf[e.from].push(e.to); });

    const ordered = topoSort(ops);
    const stages = ordered.map((o) => {
      const stage = { label: o.label, op: o.type, params: clean(o.params) };
      const next = nextOf[o.id].map((id) => labelOf[id]);
      if (next.length) stage.next = next;
      if (o.parallelism > 1) stage.parallelism = o.parallelism;
      return stage;
    });
    return {
      name: document.getElementById('job-name').value.trim(),
      source: src ? { type: src.type, params: clean(src.params) } : { type: '' },
      stages,
      sink: snk ? { type: snk.type, params: clean(snk.params) } : { type: '' },
    };
  }

  // topoSort orders operators so predecessors precede successors and the single
  // terminal leaf lands last (so its empty `next` resolves to the sink).
  function topoSort(ops) {
    const idset = new Set(ops.map((o) => o.id));
    const indeg = {}; ops.forEach((o) => (indeg[o.id] = 0));
    const adj = {}; ops.forEach((o) => (adj[o.id] = []));
    edges.forEach((e) => {
      if (idset.has(e.from) && idset.has(e.to)) { indeg[e.to]++; adj[e.from].push(e.to); }
    });
    const queue = ops.filter((o) => indeg[o.id] === 0);
    const out = [];
    while (queue.length) {
      const o = queue.shift();
      out.push(o);
      adj[o.id].forEach((to) => { if (--indeg[to] === 0) queue.push(byId(to)); });
    }
    // any cycle remnants appended (validation catches real cycles)
    ops.forEach((o) => { if (!out.includes(o)) out.push(o); });
    return out;
  }

  function clean(params) {
    const out = {};
    Object.keys(params || {}).forEach((k) => {
      const v = params[k];
      if (v === undefined || v === null || v === '') return;
      if (typeof v === 'object' && !Array.isArray(v) && Object.keys(v).length === 0) return;
      out[k] = v;
    });
    return out;
  }

  function clientValidate(spec) {
    if (!spec.name) return 'Enter a job name (top-left field)';
    if (/\s/.test(spec.name)) return 'Job name can’t contain spaces — try ' + spec.name.replace(/\s+/g, '-');
    if (!/^[A-Za-z0-9_-]+$/.test(spec.name)) return 'Job name: only letters, digits, _ and - (no dots or symbols)';
    if (!nodes.some((n) => n.kind === 'source')) return 'Add a source block';
    if (!nodes.some((n) => n.kind === 'sink')) return 'Add a sink block';
    if (!spec.stages.length) return 'Add at least one operator';
    const ops = nodes.filter((n) => n.kind === 'operator');
    const hasOut = new Set(edges.map((e) => e.from));
    const leaves = ops.filter((o) => !hasOut.has(o.id));
    if (leaves.length > 1) return 'Branches must converge: only one terminal block may feed the sink';
    return null;
  }

  function load(spec) {
    reset();
    document.getElementById('job-name').value = spec.name || '';
    // depth for layout
    const opSpecs = spec.stages || [];
    const labelToNode = {};
    // create source/operators/sink
    const src = addLayout('source', spec.source.type, spec.source.params, 0);
    opSpecs.forEach((s) => {
      const n = addLayout('operator', s.op, s.params, 1, s.label);
      n.parallelism = s.parallelism || 1;
      labelToNode[s.label] = n.id;
    });
    addLayout('sink', spec.sink.type, spec.sink.params, 2);
    // edges
    opSpecs.forEach((s) => {
      (s.next || []).forEach((t) => {
        if (labelToNode[t]) edges.push({ from: labelToNode[s.label], to: labelToNode[t] });
      });
    });
    autoLayout();
    nodes.forEach(renderNode);
    drawEdges();
    document.getElementById('canvas-hint').hidden = nodes.length > 0;
  }

  function addLayout(kind, type, params, col, label) {
    const block = Palette.find(kind, type) || { params: [] };
    const node = {
      id: 'n' + (++seq), kind, type,
      label: kind === 'operator' ? label : kind,
      params: Object.assign(Palette.defaults(block), params || {}),
      parallelism: 1,
      x: 0, y: 0, _col: col,
    };
    nodes.push(node);
    return node;
  }

  // autoLayout assigns columns by topo depth and stacks rows per column.
  function autoLayout() {
    const ops = nodes.filter((n) => n.kind === 'operator');
    const depth = {}; ops.forEach((o) => (depth[o.id] = 0));
    const adj = {}; ops.forEach((o) => (adj[o.id] = []));
    edges.forEach((e) => { if (adj[e.from]) adj[e.from].push(e.to); });
    let changed = true, guard = 0;
    while (changed && guard++ < 100) {
      changed = false;
      edges.forEach((e) => {
        if (depth[e.to] < depth[e.from] + 1) { depth[e.to] = depth[e.from] + 1; changed = true; }
      });
    }
    const maxd = Math.max(0, ...Object.values(depth));
    const colCount = {};
    const place = (n, col) => {
      colCount[col] = (colCount[col] || 0);
      n.x = 40 + col * COL_GAP;
      n.y = 40 + colCount[col] * ROW_GAP;
      colCount[col]++;
    };
    nodes.filter((n) => n.kind === 'source').forEach((n) => place(n, 0));
    ops.forEach((o) => place(o, 1 + depth[o.id]));
    nodes.filter((n) => n.kind === 'sink').forEach((n) => place(n, 2 + maxd));
  }

  function reset() {
    nodes = []; edges = []; selected = null; connecting = null;
    document.getElementById('canvas-nodes').innerHTML = '';
    document.getElementById('canvas-edges').innerHTML = '';
    document.getElementById('canvas-hint').hidden = false;
    renderInspector();
  }

  // ── actions ───────────────────────────────────────────────────────────────
  async function save() {
    const spec = serialize();
    const err = clientValidate(spec);
    if (err) { window.UI.toast(err, 'err'); return; }
    try {
      await Drift.post('/api/jobs', spec);
      window.UI.toast(`Saved "${spec.name}"`, 'ok');
      refreshJobs();
    } catch (e) { window.UI.toast('Save failed: ' + e.message, 'err'); }
  }

  async function validate() {
    const spec = serialize();
    const err = clientValidate(spec);
    if (err) { window.UI.toast(err, 'err'); return; }
    try {
      const res = await Drift.post('/api/validate', spec);
      if (res.ok) window.UI.toast('Valid ✓', 'ok');
      else window.UI.toast('Invalid: ' + res.error, 'err', 6000);
    } catch (e) { window.UI.toast('Validate failed: ' + e.message, 'err'); }
  }

  async function run() {
    const spec = serialize();
    const err = clientValidate(spec);
    if (err) { window.UI.toast(err, 'err'); return; }
    try {
      await Drift.post('/api/jobs', spec);
      await Drift.post('/api/run', { name: spec.name });
      window.UI.toast(`Running "${spec.name}"`, 'ok');
      refreshJobs();
      Monitor.setJob(spec.name);
      window.UI.showView('monitor');
    } catch (e) { window.UI.toast('Run failed: ' + e.message, 'err'); }
  }

  async function stop() {
    const name = document.getElementById('job-name').value.trim();
    try { await Drift.post('/api/stop', name ? { name } : {}); window.UI.toast(name ? `Stopped "${name}"` : 'Stopped all', 'ok'); }
    catch (e) { window.UI.toast('Stop failed: ' + e.message, 'err'); }
  }

  // ── job list ──────────────────────────────────────────────────────────────
  async function refreshJobs() {
    const list = document.getElementById('jobs-list');
    let jobs;
    try { jobs = await Drift.get('/api/jobs'); }
    catch (e) { list.innerHTML = `<span class="placeholder-text">${esc(e.message)}</span>`; return; }
    if (!jobs.length) { list.innerHTML = '<span class="placeholder-text">No saved jobs yet.</span>'; return; }
    list.innerHTML = '';
    jobs.forEach((j) => {
      const row = document.createElement('div');
      row.className = 'job-row' + (j.valid ? '' : ' job-invalid');
      row.innerHTML = `<span class="job-name" title="${esc(j.error || '')}">${esc(j.name)}</span>
        <span class="job-actions">
          <button type="button" class="job-act" data-act="dup" title="duplicate">⧉</button>
          <button type="button" class="job-act" data-act="del" title="delete">🗑</button>
        </span>`;
      row.querySelector('.job-name').addEventListener('click', () => openJob(j.name));
      row.querySelector('[data-act=dup]').addEventListener('click', () => dupJob(j.name));
      row.querySelector('[data-act=del]').addEventListener('click', () => delJob(j.name));
      list.appendChild(row);
    });
  }

  async function openJob(name) {
    try { const spec = await Drift.get('/api/jobs/' + encodeURIComponent(name)); load(spec); window.UI.toast(`Loaded "${name}"`, 'info'); }
    catch (e) { window.UI.toast('Load failed: ' + e.message, 'err'); }
  }
  async function dupJob(name) {
    const nn = prompt('Duplicate as:', name + '-copy');
    if (!nn) return;
    try { await Drift.post(`/api/jobs/${encodeURIComponent(name)}/duplicate`, { new_name: nn }); refreshJobs(); }
    catch (e) { window.UI.toast('Duplicate failed: ' + e.message, 'err'); }
  }
  async function delJob(name) {
    if (!confirm(`Delete job "${name}"?`)) return;
    try { await Drift.del('/api/jobs/' + encodeURIComponent(name)); refreshJobs(); }
    catch (e) { window.UI.toast('Delete failed: ' + e.message, 'err'); }
  }

  const byId = (id) => nodes.find((n) => n.id === id);

  return { init, refreshJobs };
})();
