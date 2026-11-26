'use strict';
/* Halcyon Heat — multiplayer server for Render (or any Node host).
   - Serves the game from ./public
   - WebSocket at /ws: rooms, live presence, and event topics (the same things the game used on claude.ai)
   - A small shared database (saves, leaderboard, shared time/weather) kept in ./data/db.json
   No npm packages needed: plain Node 18+. */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.resolve(process.env.PUBLIC_DIR || path.join(__dirname, 'public'));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DB_FILE = path.join(DATA_DIR, 'db.json');
const MAX_MSG = 64 * 1024;          // biggest message a client may send
const MAX_PLAYERS = 64;             // per room
const TOPICS = new Set(['hit', 'fx', 'chat', 'kill']);

// ------------------------------------------------------------------ static files
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css', '.json': 'application/json',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.woff2': 'font/woff2', '.glb': 'model/gltf-binary' };
function serveStatic(req, res) {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/' || p === '') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Cache-Control': p.endsWith('.html') ? 'no-cache' : 'public, max-age=300' });
    fs.createReadStream(file).pipe(res);
  });
}
const server = http.createServer((req, res) => {
  if (req.url === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }
  serveStatic(req, res);
});

// ------------------------------------------------------------------ tiny database (JSON file)
let DB = {};
try { fs.mkdirSync(DATA_DIR, { recursive: true }); DB = JSON.parse(fs.readFileSync(DB_FILE, 'utf8')); } catch (e) { DB = {}; }
let dbDirty = false;
setInterval(() => { if (!dbDirty) return; dbDirty = false; fs.writeFile(DB_FILE + '.tmp', JSON.stringify(DB), (e) => { if (!e) fs.rename(DB_FILE + '.tmp', DB_FILE, () => { }); }); }, 3000);
const validPath = (p) => typeof p === 'string' && p.length < 200 && /^[\w\-./]+$/.test(p) && !p.includes('..') && p.split('/').length % 2 === 0;
// who may write where: your own save, your own leaderboard row, and the shared world doc
function canWrite(uid, p) {
  const s = p.split('/');
  if (s[0] === 'data' && s[1] === 'users') return s[2] === uid;
  if (s[0] === 'leaderboard') return s.length === 2 && s[1] === uid;
  if (p === 'live/world') return true;
  return false;
}
function canRead(uid, p) { const s = p.split('/'); if (s[0] === 'data' && s[1] === 'users') return s[2] === uid; return true; }
const watchers = new Map();   // doc path -> Set of { client, sub }
function notify(p) { const set = watchers.get(p); if (!set) return; for (const w of set) send(w.client, { t: 'snap', sub: w.sub, path: p, data: DB[p] === undefined ? null : DB[p] }); }

// ------------------------------------------------------------------ WebSocket (RFC 6455, text frames)
const rooms = new Map();      // room name -> Set of clients
let nextPeer = 1;
server.on('upgrade', (req, socket) => {
  if ((req.url || '').split('?')[0] !== '/ws' || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const key = req.headers['sec-websocket-key']; if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const client = { socket, peer: 'p' + (nextPeer++) + '-' + crypto.randomBytes(3).toString('hex'), uid: null, room: null, presence: null, buf: Buffer.alloc(0), frag: [], alive: true, rate: 0, rateT: Date.now(), subs: new Map() };
  socket.on('data', (d) => { client.buf = Buffer.concat([client.buf, d]); if (client.buf.length > MAX_MSG * 4) return drop(client); readFrames(client); });
  socket.on('close', () => drop(client)); socket.on('error', () => drop(client));
  send(client, { t: 'hello', peer: client.peer });
});
function readFrames(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0; let len = b[1] & 0x7f, o = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); o = 4; }
    else if (len === 127) { if (b.length < 10) return; const hi = b.readUInt32BE(2), lo = b.readUInt32BE(6); if (hi) return drop(c); len = lo; o = 10; }
    if (len > MAX_MSG) return drop(c);
    if (!masked) return drop(c);
    if (b.length < o + 4 + len) return;
    const mask = b.slice(o, o + 4), data = Buffer.from(b.slice(o + 4, o + 4 + len)); for (let i = 0; i < data.length; i++) data[i] ^= mask[i & 3];
    c.buf = b.slice(o + 4 + len);
    if (op === 0x8) return drop(c);
    if (op === 0x9) { rawSend(c, 0xA, data); continue; }
    if (op === 0xA) { c.alive = true; continue; }
    if (op === 0x1 || op === 0x0) { c.frag.push(data); if (!fin) { if (c.frag.reduce((a, x) => a + x.length, 0) > MAX_MSG) return drop(c); continue; } const msg = Buffer.concat(c.frag).toString('utf8'); c.frag = []; handle(c, msg); }
  }
}
function rawSend(c, op, payload) {
  if (!c.socket || c.socket.destroyed) return; const n = payload.length; let h;
  if (n < 126) { h = Buffer.from([0x80 | op, n]); } else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(n, 2); } else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(n, 6); }
  try { c.socket.write(Buffer.concat([h, payload])); } catch (e) { }
}
function send(c, obj) { rawSend(c, 0x1, Buffer.from(JSON.stringify(obj))); }
function drop(c) {
  if (c.dead) return; c.dead = true; try { c.socket.destroy(); } catch (e) { }
  for (const [p] of c.subs) { const set = watchers.get(p); if (set) { for (const w of set) if (w.client === c) set.delete(w); if (!set.size) watchers.delete(p); } }
  leave(c);
}
function leave(c) { if (!c.room) return; const r = rooms.get(c.room); c.room = null; if (r) { r.delete(c); if (!r.size) rooms.delete(c.roomName); else peersChanged(r); } }
// presence changes are batched: everyone in the room gets the full list at most 20 times a second
function peersChanged(r) { if (r._pending) return; r._pending = setTimeout(() => { r._pending = null; const list = [...r].map(x => ({ peer: x.peer, kind: 'viewer', presence: x.presence })); for (const x of r) send(x, { t: 'peers', peers: list }); }, 50); }

function handle(c, text) {
  const now = Date.now(); if (now - c.rateT > 1000) { c.rateT = now; c.rate = 0; } if (++c.rate > 120) return;   // flood guard
  let m; try { m = JSON.parse(text); } catch (e) { return; } if (!m || typeof m !== 'object') return;
  switch (m.t) {
    case 'auth': if (typeof m.uid === 'string' && /^[\w-]{8,64}$/.test(m.uid)) c.uid = m.uid; return;
    case 'join': {
      const name = typeof m.room === 'string' && /^[\w-]{1,40}$/.test(m.room) ? m.room : 'lobby'; leave(c);
      let r = rooms.get(name); if (!r) { r = new Set(); rooms.set(name, r); }
      if (r.size >= MAX_PLAYERS) return send(c, { t: 'full' });
      r.add(c); c.room = name; c.roomName = name; send(c, { t: 'joined', room: name, peer: c.peer }); peersChanged(r); return;
    }
    case 'leave': { const r = rooms.get(c.room); leave(c); return; }
    case 'presence': { if (!c.room) return; c.presence = m.p && typeof m.p === 'object' ? m.p : null; peersChanged(rooms.get(c.room)); return; }
    case 'emit': {
      if (!c.room || !TOPICS.has(m.topic)) return; const r = rooms.get(c.room); if (!r) return;
      for (const x of r) send(x, { t: 'msg', topic: m.topic, peer: c.peer, data: m.data, sameTab: x === c });
      return;
    }
    case 'get': { if (!validPath(m.path) || !canRead(c.uid, m.path)) return send(c, { t: 'res', id: m.id, err: 'denied' }); return send(c, { t: 'res', id: m.id, data: DB[m.path] === undefined ? null : DB[m.path] }); }
    case 'set': case 'update': case 'delete': {
      if (!c.uid || !validPath(m.path) || !canWrite(c.uid, m.path)) return send(c, { t: 'res', id: m.id, err: 'denied' });
      if (m.t !== 'delete' && JSON.stringify(m.data || {}).length > 32 * 1024) return send(c, { t: 'res', id: m.id, err: 'too big' });
      if (m.t === 'set') DB[m.path] = m.data; else if (m.t === 'update') DB[m.path] = Object.assign({}, DB[m.path] || {}, m.data); else delete DB[m.path];
      dbDirty = true; notify(m.path); return send(c, { t: 'res', id: m.id, ok: true });
    }
    case 'query': {   // a collection: optional orderBy field/dir and limit
      if (typeof m.path !== 'string' || !/^[\w\-./]+$/.test(m.path) || m.path.split('/').length % 2 !== 1) return send(c, { t: 'res', id: m.id, err: 'bad' });
      const pre = m.path + '/'; let rows = Object.keys(DB).filter(k => k.startsWith(pre) && !k.slice(pre.length).includes('/') && canRead(c.uid, k)).map(k => ({ id: k.slice(pre.length), data: DB[k] }));
      if (Array.isArray(m.where)) for (const [f, op, v] of m.where) rows = rows.filter(r => { const x = r.data && r.data[f]; return op === '==' ? x === v : op === '>' ? x > v : op === '<' ? x < v : op === '>=' ? x >= v : op === '<=' ? x <= v : op === '!=' ? x !== v : true; });
      if (m.order) { const [f, dir] = m.order; rows.sort((a, b) => { const x = a.data && a.data[f], y = b.data && b.data[f]; return (x > y ? 1 : x < y ? -1 : 0) * (dir === 'desc' ? -1 : 1); }); }
      rows = rows.slice(0, Math.min(200, m.limit || 200)); return send(c, { t: 'res', id: m.id, rows });
    }
    case 'watch': { if (!validPath(m.path) || !canRead(c.uid, m.path)) return; let set = watchers.get(m.path); if (!set) { set = new Set(); watchers.set(m.path, set); } set.add({ client: c, sub: m.sub }); c.subs.set(m.path, true); send(c, { t: 'snap', sub: m.sub, path: m.path, data: DB[m.path] === undefined ? null : DB[m.path] }); return; }
  }
}
// keep connections alive through Render's proxy and clear out dead ones
setInterval(() => { for (const r of rooms.values()) for (const c of r) { if (!c.alive) { drop(c); continue; } c.alive = false; rawSend(c, 0x9, Buffer.alloc(0)); } }, 25000);

server.listen(PORT, () => console.log('Halcyon Heat server on port ' + PORT));
