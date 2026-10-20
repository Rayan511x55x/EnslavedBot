#!/usr/bin/env node
/**
 * Discord userbot base — single file, zero dependencies.
 * Requires Node.js 22+ (native fetch + WebSocket).
 *
 * ⚠️  Self-botting violates Discord's ToS. Educational use only.
 */

'use strict';

const fs = require('fs');
const vm = require('vm');
const util = require('util');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIG (tiny .env parser — no dotenv dep)
// ═══════════════════════════════════════════════════════════════════════════

if (fs.existsSync('.env')) {
  for (const line of fs.readFileSync('.env', 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*)\s*$/i);
    if (m && process.env[m[1]] === undefined) {
      process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
    }
  }
}

const CONFIG = {
  token: process.env.TOKEN,
  ownerId: process.env.OWNER_ID,
  prefix: process.env.PREFIX || '.',
  logLevel: (process.env.LOG_LEVEL || 'info').toLowerCase(),
  allowedGuilds: (process.env.ALLOWED_GUILDS || '')
    .split(',').map(s => s.trim()).filter(Boolean),
};

if (!CONFIG.token || !CONFIG.ownerId) {
  console.error('Missing TOKEN or OWNER_ID. Set them via .env or environment.');
  process.exit(1);
}

// ═══════════════════════════════════════════════════════════════════════════
// LOGGER (token-redacting)
// ═══════════════════════════════════════════════════════════════════════════

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 100 };
const CURRENT_LEVEL = LEVELS[CONFIG.logLevel] ?? LEVELS.info;

function log(level, ...args) {
  if ((LEVELS[level] ?? LEVELS.info) < CURRENT_LEVEL) return;
  const body = args
    .map(a => (typeof a === 'string' ? a : util.inspect(a, { depth: 3, colors: false })))
    .join(' ')
    .split(CONFIG.token).join('[REDACTED]');
  const line = `[${new Date().toISOString()}] [${level.toUpperCase()}] ${body}`;
  (level === 'warn' || level === 'error' ? console.error : console.log)(line);
}

// ═══════════════════════════════════════════════════════════════════════════
// REST CLIENT (rate-limit aware, 429 retry)
// ═══════════════════════════════════════════════════════════════════════════

const API_BASE = 'https://discord.com/api/v10';
const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

const SUPER_PROPERTIES = Buffer.from(JSON.stringify({
  os: 'Windows', browser: 'Chrome', device: '',
  system_locale: 'en-US',
  browser_user_agent: USER_AGENT, browser_version: '120.0.0.0',
  os_version: '10', release_channel: 'stable',
  client_build_number: 240000, client_event_source: null,
})).toString('base64');

const buckets = new Map();
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function api(method, endpoint, body) {
  const routeKey = `${method} ${endpoint.split('?')[0].replace(/\d{17,20}/g, ':id')}`;

  for (let attempt = 0; attempt < 5; attempt++) {
    const b = buckets.get(routeKey);
    const now = Date.now();
    if (b && b.remaining <= 0 && b.resetAt > now) await sleep(b.resetAt - now + 100);

    const res = await fetch(API_BASE + endpoint, {
      method,
      headers: {
        Authorization: CONFIG.token,
        'Content-Type': 'application/json',
        'User-Agent': USER_AGENT,
        'X-Super-Properties': SUPER_PROPERTIES,
        'X-Discord-Locale': 'en-US',
        'X-Discord-Timezone': 'Etc/UTC',
        Accept: '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });

    const remaining = parseInt(res.headers.get('x-ratelimit-remaining') ?? '-1', 10);
    const resetAfter = parseFloat(res.headers.get('x-ratelimit-reset-after') ?? '0');
    buckets.set(routeKey, { remaining, resetAt: Date.now() + resetAfter * 1000 });

    if (res.status === 429) {
      const data = await res.json().catch(() => ({}));
      const wait = Math.ceil((data.retry_after || resetAfter || 1) * 1000) + 100;
      log('warn', `429 on ${routeKey}, waiting ${wait}ms`);
      await sleep(wait);
      continue;
    }

    if (!res.ok) {
      const text = await res.text().catch(() => '');
      throw new Error(`${method} ${endpoint} -> ${res.status}: ${text.slice(0, 200)}`);
    }

    if (res.status === 204) return null;
    const ct = res.headers.get('content-type') || '';
    return ct.includes('json') ? res.json() : res.text();
  }

  throw new Error(`Exhausted retries on ${routeKey}`);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECURITY (owner gate, rate limit, per-command cooldown)
// ═══════════════════════════════════════════════════════════════════════════

const cooldowns = new Map();
const rateBuckets = new Map();
const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 20;

function checkRateLimit(userId) {
  const now = Date.now();
  let b = rateBuckets.get(userId);
  if (!b || now > b.reset) {
    b = { count: 0, reset: now + RATE_WINDOW_MS };
    rateBuckets.set(userId, b);
  }
  return ++b.count <= RATE_MAX;
}

function checkCooldown(userId, cmdName, ms) {
  if (!ms || ms <= 0) return 0;
  const key = `${userId}:${cmdName}`;
  const now = Date.now();
  const until = cooldowns.get(key) || 0;
  if (now < until) return until - now;
  cooldowns.set(key, now + ms);
  return 0;
}

// ═══════════════════════════════════════════════════════════════════════════
// GATEWAY (raw WebSocket, heartbeat, resume, reconnect)
// ═══════════════════════════════════════════════════════════════════════════

const GATEWAY_URL = 'wss://gateway.discord.gg/?v=10&encoding=json';

let ws = null;
let heartbeatTimer = null;
let reconnectTimer = null;
let seq = null;
let sessionId = null;
let resumeUrl = null;
let lastHeartbeatAcked = true;
let botUser = null;

const FATAL_CLOSE_CODES = new Set([4004, 4010, 4011, 4012, 4013, 4014]);

function connectGateway() {
  const url = resumeUrl || GATEWAY_URL;
  log('info', `Connecting to gateway${resumeUrl ? ' (resume)' : ''}…`);

  ws = new WebSocket(url);

  ws.addEventListener('open', () => log('debug', 'WebSocket opened'));
  ws.addEventListener('message', e => {
    let packet;
    try { packet = JSON.parse(e.data); } catch { return; }
    handleGatewayPacket(packet);
  });
  ws.addEventListener('close', e => {
    log('warn', `Gateway closed: ${e.code} ${e.reason || ''}`);
    if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
    if (FATAL_CLOSE_CODES.has(e.code)) {
      log('error', 'Fatal close code — authentication/authorization issue. Exiting.');
      process.exit(1);
    }
    scheduleReconnect();
  });
  ws.addEventListener('error', () => { /* close will fire */ });
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  const delay = 1000 + Math.random() * 4000;
  log('info', `Reconnecting in ${Math.round(delay)}ms`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectGateway();
  }, delay);
}

function sendGateway(payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify(payload));
}

function sendHeartbeat() {
  if (!lastHeartbeatAcked) {
    log('warn', 'Missed heartbeat ACK, reconnecting');
    try { ws?.close(4000); } catch {}
    return;
  }
  lastHeartbeatAcked = false;
  sendGateway({ op: 1, d: seq });
}

function startHeartbeat(interval) {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  lastHeartbeatAcked = true;
  setTimeout(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    sendHeartbeat();
    heartbeatTimer = setInterval(sendHeartbeat, interval);
  }, Math.random() * interval);
}

function sendIdentify(resume) {
  if (resume && sessionId) {
    sendGateway({
      op: 6,
      d: { token: CONFIG.token, session_id: sessionId, seq },
    });
    return;
  }
  sendGateway({
    op: 2,
    d: {
      token: CONFIG.token,
      capabilities: 125,
      properties: {
        os: 'Windows', browser: 'Chrome', device: '',
        system_locale: 'en-US',
        browser_user_agent: USER_AGENT, browser_version: '120.0.0.0',
        os_version: '10', release_channel: 'stable',
        client_build_number: 240000,
      },
      compress: false,
    },
  });
}

function handleGatewayPacket({ op, d, s, t }) {
  if (typeof s === 'number') seq = s;

  switch (op) {
    case 10: // HELLO
      log('debug', `HELLO (heartbeat=${d.heartbeat_interval}ms)`);
      startHeartbeat(d.heartbeat_interval);
      sendIdentify(!!resumeUrl);
      break;
    case 11: // HEARTBEAT ACK
      lastHeartbeatAcked = true;
      break;
    case 1: // HEARTBEAT request from server
      sendHeartbeat();
      break;
    case 7: // RECONNECT
      log('warn', 'Server requested reconnect');
      try { ws?.close(4000); } catch {}
      break;
    case 9: // INVALID_SESSION
      log('warn', 'Invalid session — starting fresh');
      resumeUrl = null;
      sessionId = null;
      try { ws?.close(4000); } catch {}
      break;
    case 0: // DISPATCH
      handleDispatch(t, d);
      break;
  }
}

function handleDispatch(type, data) {
  switch (type) {
    case 'READY':
      botUser = data.user;
      sessionId = data.session_id;
      resumeUrl = data.resume_gateway_url || GATEWAY_URL;
      log('info', `READY as ${data.user.username} (${data.user.id})`);
      log('info', `Guilds: ${data.guilds.length}, DM channels: ${data.private_channels.length}`);
      break;
    case 'RESUMED':
      log('info', 'Session resumed');
      break;
    case 'MESSAGE_CREATE':
      handleMessage(data).catch(err => log('error', 'Message handler:', err.message));
      break;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// MESSAGE HANDLER
// ═══════════════════════════════════════════════════════════════════════════

async function handleMessage(msg) {
  if (!msg.author || msg.author.bot || msg.webhook_id) return;
  if (msg.author.id !== CONFIG.ownerId) return;
  if (CONFIG.allowedGuilds.length && msg.guild_id && !CONFIG.allowedGuilds.includes(msg.guild_id)) return;
  if (!msg.content?.startsWith(CONFIG.prefix)) return;
  if (!checkRateLimit(msg.author.id)) return;

  const body = msg.content.slice(CONFIG.prefix.length).trim();
  const [rawName, ...args] = body.split(/\s+/);
  if (!rawName) return;

  const cmd = commands.get(rawName.toLowerCase());
  if (!cmd) return;

  if (cmd.ownerOnly && msg.author.id !== CONFIG.ownerId) return;

  const remaining = checkCooldown(msg.author.id, cmd.name, cmd.cooldown ?? 3000);
  if (remaining > 0) {
    log('debug', `Cooldown ${cmd.name}: ${remaining}ms left`);
    return;
  }

  const reply = content => replyTo(msg, content);

  try {
    await cmd.execute({ message: msg, args, reply, api, commands, log, client: { user: botUser } });
  } catch (err) {
    log('error', `Command "${cmd.name}" threw:`, err.message);
    reply(`Error: \`${String(err.message || err).slice(0, 200)}\``).catch(() => {});
  }
}

async function replyTo(msg, content) {
  return api('POST', `/channels/${msg.channel_id}/messages`, {
    content,
    message_reference: { message_id: msg.id, channel_id: msg.channel_id, guild_id: msg.guild_id },
    allowed_mentions: { replied_user: false, parse: [] },
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// COMMANDS
// ═══════════════════════════════════════════════════════════════════════════

const commands = new Map();

function defineCommand(def) {
  if (!def?.name || typeof def.execute !== 'function') throw new Error('Bad command def');
  commands.set(def.name, def);
  for (const a of def.aliases || []) commands.set(a, def);
}

defineCommand({
  name: 'ping',
  description: 'Measure round-trip latency.',
  cooldown: 2000,
  async execute({ reply, api }) {
    const start = Date.now();
    const sent = await reply('pong');
    const rtt = Date.now() - start;
    await api('PATCH', `/channels/${sent.channel_id}/messages/${sent.id}`, {
      content: `pong — ${rtt}ms`,
    });
  },
});

defineCommand({
  name: 'help',
  aliases: ['h', 'commands'],
  description: 'List available commands.',
  cooldown: 2000,
  async execute({ reply }) {
    const unique = [...new Set(commands.values())].sort((a, b) => a.name.localeCompare(b.name));
    const lines = unique.map(c => {
      const alias = c.aliases?.length ? ` (${c.aliases.join(', ')})` : '';
      const flag = c.ownerOnly ? ' 🔒' : '';
      return `\`${CONFIG.prefix}${c.name}\`${alias}${flag} — ${c.description || 'No description'}`;
    });
    await reply(lines.join('\n'));
  },
});

defineCommand({
  name: 'eval',
  aliases: ['ev'],
  description: 'Evaluate JS in a sandbox (owner only).',
  ownerOnly: true,
  cooldown: 0,
  async execute({ args, reply, message }) {
    const code = args.join(' ').trim();
    if (!code) return reply('Provide code to evaluate.');
    const ctx = vm.createContext({
      message, api, commands, log, config: CONFIG, botUser,
      require, process, console, setTimeout, setInterval, clearTimeout,
    });
    let result;
    try {
      result = vm.runInContext(code, ctx, { timeout: 5000 });
      if (result && typeof result.then === 'function') {
        result = await Promise.race([
          result,
          new Promise((_, rej) => setTimeout(() => rej(new Error('Timed out')), 5000)),
        ]);
      }
    } catch (err) {
      return reply('```js\n' + String(err?.stack || err).slice(0, 1900) + '\n```');
    }
    const out = typeof result === 'string' ? result : util.inspect(result, { depth: 1 });
    await reply('```js\n' + String(out).slice(0, 1900) + '\n```');
  },
});

defineCommand({
  name: 'raw',
  aliases: ['api'],
  description: 'Raw API call: .raw GET /users/@me',
  ownerOnly: true,
  cooldown: 0,
  async execute({ args, reply }) {
    const [method, endpoint, ...rest] = args;
    if (!method || !endpoint) return reply('usage: .raw <METHOD> <path> [json]');
    let body;
    if (rest.length) {
      try { body = JSON.parse(rest.join(' ')); }
      catch { body = { content: rest.join(' ') }; }
    }
    try {
      const res = await api(method.toUpperCase(), endpoint, body);
      await reply('```json\n' + JSON.stringify(res, null, 2).slice(0, 1900) + '\n```');
    } catch (err) {
      await reply('Error: ' + String(err.message).slice(0, 1900));
    }
  },
});

// ═══════════════════════════════════════════════════════════════════════════
// BOOT
// ═══════════════════════════════════════════════════════════════════════════

process.on('unhandledRejection', r => log('error', 'Unhandled rejection:', r?.message || r));
process.on('uncaughtException', e => log('error', 'Uncaught exception:', e?.message || e));

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    log('info', `Received ${sig}, shutting down`);
    try { ws?.close(1000); } catch {}
    process.exit(0);
  });
}

log('info', `Commands: ${[...new Set(commands.values())].map(c => c.name).join(', ')}`);
connectGateway();
