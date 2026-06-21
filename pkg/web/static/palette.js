'use strict';

// Palette — loads the block catalog from /api/palette and renders the draggable
// palette plus the per-block param forms used by the builder inspector.
const Palette = (() => {
  let cat = { sources: [], operators: [], sinks: [] };

  async function load() {
    cat = await Drift.get('/api/palette');
    return cat;
  }

  // find returns the BlockDef for a kind ("source"|"operator"|"sink") + type.
  function find(kind, type) {
    const list = kind === 'source' ? cat.sources : kind === 'sink' ? cat.sinks : cat.operators;
    return (list || []).find((b) => b.type === type);
  }

  // renderPalette fills the palette element with draggable items grouped by kind.
  function renderPalette(el) {
    el.innerHTML = '';
    const group = (title, blocks, kind) => {
      const h = document.createElement('p');
      h.className = 'panel-eyebrow';
      h.textContent = title;
      el.appendChild(h);
      (blocks || []).forEach((b) => {
        const item = document.createElement('div');
        item.className = 'palette-item kind-' + kind;
        item.draggable = true;
        item.dataset.kind = kind;
        item.dataset.type = b.type;
        item.innerHTML = `<span class="pi-type">${b.type}</span>` +
          (b.flusher ? `<span class="pi-tag">window</span>` : '');
        item.title = b.doc || '';
        item.addEventListener('dragstart', (e) => {
          e.dataTransfer.setData('text/plain', JSON.stringify({ kind, type: b.type }));
        });
        const help = Help.button({ title: b.type, doc: b.doc, subject: `${kind}: ${b.type}` });
        help.draggable = false;
        item.appendChild(help);
        el.appendChild(item);
      });
    };
    group('sources', cat.sources, 'source');
    group('operators', cat.operators, 'operator');
    group('sinks', cat.sinks, 'sink');
  }

  // defaults returns a params object with each param's default applied.
  function defaults(block) {
    const p = {};
    (block.params || []).forEach((par) => {
      if (par.default !== undefined && par.default !== null) p[par.name] = par.default;
    });
    return p;
  }

  // renderForm builds a params form for a block; onChange(params) fires on edits.
  function renderForm(block, params, onChange) {
    const wrap = document.createElement('div');
    wrap.className = 'param-form';
    if (!block.params || !block.params.length) {
      const n = document.createElement('p');
      n.className = 'placeholder-text';
      n.textContent = 'No parameters.';
      wrap.appendChild(n);
      return wrap;
    }
    block.params.forEach((par) => {
      const row = document.createElement('label');
      row.className = 'param-row';
      const name = document.createElement('span');
      name.className = 'param-name';
      name.textContent = par.name + (par.required ? ' *' : '');
      name.title = par.doc || '';
      row.appendChild(name);
      row.appendChild(buildInput(par, params, onChange));
      if (par.doc) {
        const hint = document.createElement('span');
        hint.className = 'param-hint';
        hint.textContent = par.doc;
        row.appendChild(hint);
      }
      wrap.appendChild(row);
    });
    return wrap;
  }

  function buildInput(par, params, onChange) {
    const cur = params[par.name];
    let input;
    if (par.kind === 'bool') {
      input = document.createElement('input');
      input.type = 'checkbox';
      input.className = 'param-check';
      input.checked = cur === true;
      input.addEventListener('change', () => { params[par.name] = input.checked; onChange(params); });
    } else if (par.kind === 'enum') {
      input = document.createElement('select');
      input.className = 'param-input';
      (par.enum || []).forEach((opt) => {
        const o = document.createElement('option');
        o.value = opt; o.textContent = opt;
        if (opt === cur) o.selected = true;
        input.appendChild(o);
      });
      input.addEventListener('change', () => { params[par.name] = input.value; onChange(params); });
    } else if (par.kind === 'map') {
      input = buildMapEditor(par, params, onChange);
    } else {
      input = document.createElement('input');
      input.type = 'text';
      input.className = 'param-input';
      input.placeholder = placeholderFor(par.kind);
      if (cur !== undefined && cur !== null) input.value = String(cur);
      input.addEventListener('input', () => {
        params[par.name] = coerce(par.kind, input.value);
        onChange(params);
      });
    }
    return input;
  }

  // buildMapEditor edits a string→value map (e.g. generator fields).
  function buildMapEditor(par, params, onChange) {
    const box = document.createElement('div');
    box.className = 'map-editor';
    let map = (params[par.name] && typeof params[par.name] === 'object') ? params[par.name] : {};
    params[par.name] = map;

    const redraw = () => {
      box.innerHTML = '';
      Object.keys(map).forEach((k) => {
        const r = document.createElement('div');
        r.className = 'map-row';
        const kk = document.createElement('input');
        kk.className = 'param-input map-key'; kk.value = k;
        const vv = document.createElement('input');
        vv.className = 'param-input map-val'; vv.value = String(map[k]);
        const del = document.createElement('button');
        del.type = 'button'; del.className = 'map-del'; del.textContent = '×';
        kk.addEventListener('change', () => {
          const nv = map[k]; delete map[k]; if (kk.value) map[kk.value] = nv; onChange(params); redraw();
        });
        vv.addEventListener('input', () => { map[k] = vv.value; onChange(params); });
        del.addEventListener('click', () => { delete map[k]; onChange(params); redraw(); });
        r.append(kk, vv, del);
        box.appendChild(r);
      });
      const add = document.createElement('button');
      add.type = 'button'; add.className = 'map-add'; add.textContent = '+ field';
      add.addEventListener('click', () => {
        let i = 1, key = 'field'; while (map[key]) key = 'field' + (++i);
        map[key] = ''; onChange(params); redraw();
      });
      box.appendChild(add);
    };
    redraw();
    return box;
  }

  // placeholderFor returns an example hint for a text input by kind.
  function placeholderFor(kind) {
    switch (kind) {
      case 'duration': return 'e.g. 500ms, 1s, 2m';
      case 'int': return 'e.g. 10';
      case 'number': return 'e.g. 10';
      case 'any': return 'value';
      default: return '';
    }
  }

  // coerce turns a text value into the right JS type for the param kind.
  function coerce(kind, raw) {
    if (raw === '') return undefined;
    if (kind === 'int' || kind === 'number') {
      const n = Number(raw);
      return Number.isNaN(n) ? raw : n;
    }
    if (kind === 'any') {
      if (raw === 'true') return true;
      if (raw === 'false') return false;
      const n = Number(raw);
      if (!Number.isNaN(n) && raw.trim() !== '') return n;
      return raw;
    }
    return raw; // string, duration
  }

  return { load, find, renderPalette, renderForm, defaults };
})();
