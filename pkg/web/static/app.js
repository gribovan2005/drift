'use strict';

// app — boot, view routing, status polling, and shared UI helpers (toasts,
// reconnect banner). Detects whether the server is a control plane (has a
// runner) and reveals the Builder view accordingly.

window.UI = (() => {
  function toast(msg, type = 'info', ms = 3200) {
    const stack = document.getElementById('toast-stack');
    const t = document.createElement('div');
    t.className = 'toast toast-' + type;
    t.textContent = msg;
    stack.appendChild(t);
    setTimeout(() => { t.classList.add('leaving'); setTimeout(() => t.remove(), 250); }, ms);
  }
  function reconnecting(on) {
    document.getElementById('reconnect-banner').hidden = !on;
  }
  function showView(name) {
    document.querySelectorAll('.view').forEach((v) => { v.hidden = v.id !== 'view-' + name; });
    document.querySelectorAll('.nav-btn').forEach((b) => b.classList.toggle('is-active', b.dataset.view === name));
    if (name === 'builder') Builder.refreshJobs();
  }
  return { toast, reconnecting, showView };
})();

document.addEventListener('DOMContentLoaded', () => {
  startClock();
  setupNav();
  setupTokenButton();
  Monitor.init();
  detectControlPlane();
});

function startClock() {
  const el = document.getElementById('clock');
  const tick = () => { el.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false }); };
  tick();
  setInterval(tick, 1000);
}

function setupNav() {
  document.getElementById('nav').addEventListener('click', (e) => {
    const btn = e.target.closest('.nav-btn');
    if (btn && !btn.hidden) window.UI.showView(btn.dataset.view);
  });
}

function setupTokenButton() {
  document.getElementById('token-btn').addEventListener('click', () => {
    const cur = Drift.token();
    const v = prompt('API token (blank to clear):', cur);
    if (v === null) return;
    Drift.setToken(v.trim());
    location.reload();
  });
}

// detectControlPlane reveals the Builder view + status pill when /api/status
// exists (i.e. the server was started with a runner). Otherwise stays monitor-only.
async function detectControlPlane() {
  try {
    await Drift.get('/api/status');
  } catch (e) {
    // Monitor-only mode (drift run --ui / demo): no builder.
    const sub = document.getElementById('monitor-empty-sub');
    if (sub) sub.textContent = 'Start a pipeline with the CLI.';
    return;
  }
  document.getElementById('nav-builder').hidden = false;
  document.getElementById('status-pill').hidden = false;
  document.getElementById('job-select').addEventListener('change', (e) => Monitor.setJob(e.target.value));
  Builder.init();
  pollStatus();
}

function pollStatus() {
  const pill = document.getElementById('status-pill');
  const bstat = document.getElementById('builder-status');
  const sel = document.getElementById('job-select');

  const refresh = async () => {
    let s;
    try { s = await Drift.get('/api/status'); } catch (e) { return; }
    const running = s.running || [];

    pill.className = 'status-pill ' + (running.length ? 'running' : 'idle');
    pill.textContent = running.length ? `▶ ${running.length} running` : '■ idle';
    if (bstat) {
      bstat.textContent = running.length ? `${running.length} running` : 'idle';
      bstat.className = 'tb-status ' + (running.length ? 'running' : '');
    }

    syncSelector(sel, running);
  };
  refresh();
  setInterval(refresh, 2000);
}

// syncSelector keeps the monitor's job dropdown in step with the running set and
// ensures the monitor is always pointed at a valid (or no) job.
function syncSelector(sel, running) {
  const names = running.map((j) => j.name);
  sel.hidden = names.length === 0;

  // Rebuild options if the running set changed.
  const have = Array.from(sel.options).map((o) => o.value).join(',');
  if (have !== names.join(',')) {
    sel.innerHTML = '';
    names.forEach((n) => {
      const o = document.createElement('option');
      o.value = n; o.textContent = n;
      sel.appendChild(o);
    });
  }

  const cur = Monitor.job();
  if (!names.length) {
    if (cur) Monitor.setJob('');           // nothing running → idle
    return;
  }
  if (!names.includes(cur)) {
    sel.value = names[0];
    Monitor.setJob(names[0]);              // point at a valid job
  } else {
    sel.value = cur;
  }
}
