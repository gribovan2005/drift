'use strict';

// Help — click-to-explain popover. Layer 1 shows an instant static description
// (block docs from the catalog, or the metrics glossary below). Layer 2 is an
// "Ask AI" button that posts the element + its context to /api/ask.
const Help = (() => {
  // METRICS is the static glossary for monitor metrics (no AI needed).
  const METRICS = {
    'throughput': 'Records this stage processed per second over the last sampling window. Drops to 0 when idle or fully blocked downstream.',
    'processed total': 'Cumulative records this stage has processed since it started. Always increasing.',
    'errors': 'How many Process() calls returned an error. Should stay 0; anything above warrants a look.',
    'latency p50': 'Median time to process one batch. Your typical-case speed.',
    'latency p99': 'Time the slowest 1% of batches take. Spikes here mean tail latency / stalls even if p50 looks fine.',
    'queue depth': 'Records waiting in this stage’s input channel. Persistently high = this stage is the bottleneck (backpressure).',
    'kept': 'Share of upstream records that survived to this stage (the rest were dropped by a filter/dedup upstream).',
    'uptime': 'How long the running pipeline has been up.',
  };

  let pop = null;

  function close() {
    if (!pop) return;
    pop.remove();
    pop = null;
    document.removeEventListener('mousedown', onDoc, true);
    window.removeEventListener('resize', close);
  }
  function onDoc(e) { if (pop && !pop.contains(e.target)) close(); }

  // open shows the popover anchored to el. opts: {title, doc, subject, context, question}.
  function open(el, opts) {
    close();
    pop = document.createElement('div');
    pop.className = 'help-pop';
    pop.innerHTML = `
      <div class="help-title">${esc(opts.title || 'Help')}</div>
      <div class="help-doc">${esc(opts.doc || 'No description available.')}</div>
      <button type="button" class="help-ask">✦ Ask AI</button>
      <div class="help-answer" hidden></div>`;
    document.body.appendChild(pop);
    placeNear(pop, el);
    pop.querySelector('.help-ask').addEventListener('click', () => ask(opts));
    setTimeout(() => document.addEventListener('mousedown', onDoc, true), 0);
    window.addEventListener('resize', close);
  }

  async function ask(opts) {
    const btn = pop.querySelector('.help-ask');
    const ans = pop.querySelector('.help-answer');
    btn.disabled = true;
    btn.textContent = 'asking…';
    ans.hidden = false;
    ans.innerHTML = '<span class="placeholder-text">Asking Claude…</span>';
    try {
      const text = await Drift.post('/api/ask', {
        subject: opts.subject || opts.title || '',
        question: opts.question || '',
        context: opts.context || '',
      });
      ans.innerHTML = mini(text);
    } catch (e) {
      ans.textContent = 'Error: ' + e.message;
    } finally {
      btn.disabled = false;
      btn.textContent = '✦ Ask AI again';
    }
  }

  function placeNear(el, anchor) {
    const r = anchor.getBoundingClientRect();
    el.style.position = 'fixed';
    el.style.visibility = 'hidden';
    // measure
    const w = el.offsetWidth, h = el.offsetHeight;
    let left = r.left;
    let top = r.bottom + 6;
    if (left + w > window.innerWidth - 8) left = window.innerWidth - w - 8;
    if (top + h > window.innerHeight - 8) top = r.top - h - 6; // flip above
    el.style.left = Math.max(8, left) + 'px';
    el.style.top = Math.max(8, top) + 'px';
    el.style.visibility = 'visible';
  }

  // mini renders a tiny subset of markdown for AI answers.
  function mini(md) {
    return esc(md)
      .replace(/`([^`]+)`/g, (_, c) => `<code>${c}</code>`)
      .replace(/^[-*] (.+)$/gm, (_, t) => `<div>· ${t}</div>`)
      .replace(/\*\*([^*]+)\*\*/g, (_, t) => `<strong>${t}</strong>`)
      .replace(/\n{2,}/g, '<br><br>')
      .replace(/\n/g, '<br>');
  }

  // metricDoc returns the glossary entry for a metric label, or ''.
  function metricDoc(label) { return METRICS[label] || ''; }

  // button returns a small "?" element that opens help for opts on click.
  function button(opts) {
    const b = document.createElement('button');
    b.type = 'button';
    b.className = 'help-btn';
    b.textContent = '?';
    b.title = 'Explain';
    b.addEventListener('click', (e) => { e.stopPropagation(); open(b, opts); });
    return b;
  }

  return { open, close, button, metricDoc };
})();
