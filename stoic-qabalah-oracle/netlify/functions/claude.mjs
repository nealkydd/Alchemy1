// netlify/functions/claude.mjs
// Stoic Qabalah — Tree of Life Reader
// Netlify Functions v2  ·  ESM  ·  Streaming SSE

import { timingSafeEqual } from 'node:crypto';

const CORS = {
  'Access-Control-Allow-Origin':  '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS'
};

// Opus excluded — neither reading type uses it.
const MODEL_MAP = {
  haiku:  'claude-haiku-4-5-20251001',
  sonnet: 'claude-sonnet-4-6',
  'claude-haiku-4-5-20251001': 'claude-haiku-4-5-20251001',
  'claude-haiku-4-5':          'claude-haiku-4-5-20251001',
  'claude-sonnet-4-6':         'claude-sonnet-4-6'
};

const MAX_TOKENS_CAP = 1500;

// Belt-and-braces: if the HTML omits the model field, transit gets Sonnet,
// lens gets Haiku.
function resolveModel(payload) {
  if (payload.model && MODEL_MAP[payload.model]) return MODEL_MAP[payload.model];
  const action = payload.action || payload.task || '';
  if (action === 'tree_transit_reading' || action === 'interpret') return MODEL_MAP.sonnet;
  return MODEL_MAP.haiku;
}

export default async function handler(req) {
  if (req.method === 'OPTIONS') {
    return new Response('', { status: 200, headers: CORS });
  }
  if (req.method !== 'POST') {
    return jsonRes({ ok: false, error: 'Method not allowed' }, 405);
  }

  let payload;
  try {
    payload = await req.json();
  } catch {
    return jsonRes({ ok: false, error: 'Invalid JSON' }, 400);
  }

  const action = payload.action || payload.task || '';

  if (action === 'verify_access') return handleVerify(payload);

  if (
    action === 'tree_lens_reading'    ||
    action === 'tree_transit_reading' ||
    action === 'interpret'            ||
    action === 'hermetic_oracle'      ||
    payload.task === 'hermetic_oracle'
  ) {
    return streamReading(payload);
  }

  return jsonRes({ ok: false, error: 'Unknown action' }, 400);
}

// ── Verify access ─────────────────────────────────────────────────────────────

function handleVerify(payload) {
  const expected = (process.env.ACCESS_PASSWORD || '').trim();
  const supplied  = String(payload.key || '').trim();
  if (!expected) return jsonRes({ ok: false, error: 'Gate not configured' }, 500);

  let ok = false;
  try {
    const a = Buffer.from(expected, 'utf8');
    const b = Buffer.from(supplied,  'utf8');
    ok = a.length === b.length && timingSafeEqual(a, b);
  } catch {
    ok = false;
  }
  return jsonRes({ ok }, 200);
}

// ── Streaming reading ─────────────────────────────────────────────────────────

async function streamReading(payload) {
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) return jsonRes({ ok: false, error: 'ANTHROPIC_API_KEY not configured' }, 500);

  const model  = resolveModel(payload);
  const maxTok = Math.min(Number(payload.max_tokens || payload.maxTokens || 1000), MAX_TOKENS_CAP);
  const system = payload.system || undefined;
  const prompt = payload.prompt || buildLensPrompt(payload);

  if (!prompt) return jsonRes({ ok: false, error: 'No prompt content' }, 400);

  const ctrl    = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 25_000);

  let upstream;
  try {
    const body = {
      model,
      max_tokens: maxTok,
      stream: true,
      messages: [{ role: 'user', content: prompt }]
    };
    if (system) body.system = system;

    upstream = await fetch('https://api.anthropic.com/v1/messages', {
      method:  'POST',
      signal:  ctrl.signal,
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         apiKey,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify(body)
    });
  } catch (err) {
    clearTimeout(timeout);
    if (err.name === 'AbortError') return jsonRes({ ok: false, error: 'Upstream timeout' }, 504);
    return jsonRes({ ok: false, error: err.message || 'Fetch error' }, 502);
  }

  if (!upstream.ok) {
    clearTimeout(timeout);
    const detail = await upstream.text().catch(() => '');
    if (upstream.status === 401) return jsonRes({ ok: false, error: 'Invalid API key' }, 401);
    if (upstream.status === 429) {
      const retry = upstream.headers.get('retry-after') || '60';
      return new Response(
        JSON.stringify({ ok: false, error: 'Rate limited', retryAfter: retry }),
        { status: 429, headers: { ...CORS, 'Content-Type': 'application/json', 'Retry-After': retry } }
      );
    }
    return jsonRes({ ok: false, error: 'Anthropic error', detail }, upstream.status);
  }

  const enc = new TextEncoder();
  const sse = (event, data) =>
    enc.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);

  const outStream = new ReadableStream({
    async start(controller) {
      const reader = upstream.body.getReader();
      const dec    = new TextDecoder();

      // Declared outside the loop so state persists across TCP chunks.
      let buf      = '';
      let evType   = '';
      let doneSent = false;

      const finish = () => {
        if (!doneSent) {
          doneSent = true;
          controller.enqueue(sse('done', {}));
        }
        clearTimeout(timeout);
        controller.close();
      };

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buf += dec.decode(value, { stream: true });
          const lines = buf.split('\n');
          buf = lines.pop();

          for (const line of lines) {
            if (line === '') {
              // SSE event terminator — reset for next event block.
              evType = '';
              continue;
            }
            if (line.startsWith('event: ')) {
              evType = line.slice(7).trim();
            } else if (line.startsWith('data: ')) {
              const raw = line.slice(6).trim();
              if (!raw) continue;
              let parsed;
              try { parsed = JSON.parse(raw); } catch { continue; }

              if (evType === 'content_block_delta') {
                const text = parsed?.delta?.text;
                if (text) controller.enqueue(sse('delta', { text }));
              } else if (evType === 'message_stop') {
                finish(); return;
              } else if (evType === 'error') {
                // Anthropic mid-stream error (content filter, model error, etc.)
                controller.enqueue(
                  sse('error', { error: parsed?.error?.message || 'Upstream error' })
                );
                finish(); return;
              }
            }
          }
        }
        finish();
      } catch (err) {
        clearTimeout(timeout);
        if (err.name !== 'AbortError') {
          controller.enqueue(sse('error', { error: err.message || 'Stream error' }));
        }
        controller.close();
      }
    }
  });

  return new Response(outStream, {
    status: 200,
    headers: {
      ...CORS,
      'Content-Type':      'text/event-stream',
      'Cache-Control':     'no-cache',
      'Connection':        'keep-alive',
      'X-Accel-Buffering': 'no'
    }
  });
}

// ── Build prompt from structured lens/gate payload ────────────────────────────

function buildLensPrompt(payload) {
  const { lens, gate, quote, focus, chart, transit, name, instruction } = payload;
  if (!lens && !gate) return '';

  const lines = [];
  if (name) lines.push(`Reader: ${name}`);
  if (lens) {
    lines.push(`Lens: ${lens.sephirah} (${lens.sphere}) — ${lens.eyebrow}`);
    if (lens.question)      lines.push(`Question: ${lens.question}`);
    if (lens.participation) lines.push(`Participation: ${lens.participation}`);
  }
  if (gate) {
    const gSig = [gate.name, gate.planet && `(${gate.planet})`, gate.sign && `in ${gate.sign}`]
      .filter(Boolean).join(' ');
    lines.push(`Gate: ${gSig}`);
    if (gate.theme)            lines.push(`Theme: ${gate.theme}`);
    if (gate.virtue)           lines.push(`Virtue: ${gate.virtue}`);
    if (gate.activation)       lines.push(`Activation: ${gate.activation}`);
    if (gate.activationReason) lines.push(`Activation reason: ${gate.activationReason}`);
  }
  if (quote)   lines.push(`\nQuote in view:\n"${quote.text}" — ${quote.author} (${quote.source})`);
  if (focus)   lines.push(`\nFocus: ${focus}`);
  if (chart)   lines.push(`\nNatal chart:\n${JSON.stringify(chart, null, 2)}`);
  if (transit) lines.push(`\nTransit data:\n${JSON.stringify(transit, null, 2)}`);
  if (instruction) lines.push(`\n${instruction}`);
  return lines.join('\n');
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function jsonRes(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' }
  });
}
