'use strict';

// Drift — shared API client. Injects the optional auth token (stored in
// localStorage) as a bearer header, and as ?token= for EventSource which cannot
// set headers.
const Drift = (() => {
  const TOKEN_KEY = 'drift_token';

  const token = () => localStorage.getItem(TOKEN_KEY) || '';
  const setToken = (t) => {
    if (t) localStorage.setItem(TOKEN_KEY, t);
    else localStorage.removeItem(TOKEN_KEY);
  };

  class ApiError extends Error {
    constructor(message, status, data) {
      super(message);
      this.status = status;
      this.data = data;
    }
  }

  async function req(method, path, body) {
    const headers = {};
    const t = token();
    if (t) headers['Authorization'] = 'Bearer ' + t;
    const opts = { method, headers };
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(path, opts);
    const ct = res.headers.get('content-type') || '';
    const data = ct.includes('application/json')
      ? await res.json().catch(() => null)
      : await res.text();
    if (!res.ok) {
      const msg = (data && data.error) || (typeof data === 'string' && data) || res.statusText;
      throw new ApiError(msg, res.status, data);
    }
    return data;
  }

  // eventsURL builds the SSE URL with optional job + token query params.
  const eventsURL = (job) => {
    const q = [];
    if (job) q.push('job=' + encodeURIComponent(job));
    const t = token();
    if (t) q.push('token=' + encodeURIComponent(t));
    return '/api/events' + (q.length ? '?' + q.join('&') : '');
  };

  return {
    token, setToken, ApiError, eventsURL,
    get: (p) => req('GET', p),
    post: (p, b) => req('POST', p, b === undefined ? {} : b),
    del: (p) => req('DELETE', p),
  };
})();
