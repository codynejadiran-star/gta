'use strict';
/* Halcyon Heat — multiplayer bridge for self-hosting.
   On claude.ai the game uses Claude's built-in room, database and user services (window.claude).
   Off claude.ai those don't exist, so this file provides the same calls, backed by this site's own
   server (server.js, WebSocket at /ws). Load it before the game's scripts. */
(function () {
  if (window.claude && window.claude.use) return;   // running on claude.ai: use the real services
  const WS_URL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';
  // a stable anonymous id for this browser (saves and the leaderboard hang off it)
  let uid = null; try { uid = localStorage.getItem('hh_uid'); } catch (e) { }
  if (!uid) { uid = 'u' + Array.from(crypto.getRandomValues(new Uint8Array(12))).map(b => b.toString(16).padStart(2, '0')).join(''); try { localStorage.setItem('hh_uid', uid); } catch (e) { } }

  let ws = null, open = false, myPeer = null, nextId = 1, retry = 0, closedByUs = false;
  const queue = [], pending = new Map(), topicFns = new Map(), peerFns = [], snapFns = new Map();
  let lastPresence = null, roomName = null, lastPeers = null;
  function connect() {
    try { ws = new WebSocket(WS_URL); } catch (e) { return later(); }
    ws.onopen = () => { open = true; retry = 0; raw({ t: 'auth', uid }); if (roomName) raw({ t: 'join', room: roomName }); if (lastPresence) raw({ t: 'presence', p: lastPresence }); for (const [sub, s] of snapFns) raw({ t: 'watch', path: s.path, sub }); while (queue.length) raw(queue.shift()); };
    ws.onmessage = (ev) => { let m; try { m = JSON.parse(ev.data); } catch (e) { return; } onMsg(m); };
    ws.onclose = () => { open = false; if (!closedByUs) later(); };
    ws.onerror = () => { };
  }
  function later() { setTimeout(connect, Math.min(10000, 500 * Math.pow(2, retry++))); }
  function raw(o) { try { ws.send(JSON.stringify(o)); } catch (e) { } }
  function out(o) { if (open) raw(o); else if (queue.length < 200) queue.push(o); }
  function call(o) { return new Promise((res, rej) => { const id = nextId++; o.id = id; pending.set(id, { res, rej }); out(o); setTimeout(() => { if (pending.has(id)) { pending.delete(id); rej(new Error('timeout')); } }, 10000); }); }
  function onMsg(m) {
    if (m.t === 'hello') myPeer = m.peer;
    else if (m.t === 'joined') myPeer = m.peer;
    else if (m.t === 'peers') { lastPeers = m.peers.map(p => Object.assign({}, p, { sameTab: p.peer === myPeer })); for (const f of peerFns) try { f({ peers: lastPeers }); } catch (e) { console.error(e); } }
    else if (m.t === 'msg') { const fs = topicFns.get(m.topic); if (fs) for (const f of fs) try { f({ peer: m.peer, data: m.data, sameTab: !!m.sameTab }); } catch (e) { console.error(e); } }
    else if (m.t === 'res') { const p = pending.get(m.id); if (!p) return; pending.delete(m.id); if (m.err) p.rej(new Error(m.err)); else p.res(m); }
    else if (m.t === 'snap') { const s = snapFns.get(m.sub); if (s) try { s.fn(snap(m.path, m.data)); } catch (e) { console.error(e); } }
  }
  const snap = (p, data) => ({ id: p.split('/').pop(), exists: data !== null && data !== undefined, data: () => data });
  connect();

  // ---- room: same shape as claude.use('room')
  const roomApi = {
    join(name) { roomName = String(name || 'lobby'); out({ t: 'join', room: roomName }); return Promise.resolve(roomApi); },
    onPeers(fn) { peerFns.push(fn); if (lastPeers) setTimeout(() => fn({ peers: lastPeers }), 0); },
    on(topic, fn) { if (!topicFns.has(topic)) topicFns.set(topic, []); topicFns.get(topic).push(fn); },
    presence(p) { lastPresence = p; out({ t: 'presence', p }); return Promise.resolve(); },
    emit(topic, data) { out({ t: 'emit', topic, data }); return Promise.resolve(); },
    leave() { roomName = null; out({ t: 'leave' }); return Promise.resolve(); }
  };
  if (!roomName) roomApi.join('lobby');
  // ---- db: doc/collection with get/set/update/delete/onSnapshot, orderBy/where/limit
  function doc(path) {
    return { id: path.split('/').pop(), path,
      get: () => call({ t: 'get', path }).then(r => snap(path, r.data)),
      set: (data) => call({ t: 'set', path, data }).then(() => undefined),
      update: (data) => call({ t: 'update', path, data }).then(() => undefined),
      delete: () => call({ t: 'delete', path }).then(() => undefined),
      onSnapshot(fn, err) { const sub = 's' + (nextId++); snapFns.set(sub, { path, fn, err }); out({ t: 'watch', path, sub }); return () => snapFns.delete(sub); } };
  }
  function query(path, q) {
    return { orderBy: (f, dir) => query(path, Object.assign({}, q, { order: [f, dir || 'asc'] })), where: (f, op, v) => query(path, Object.assign({}, q, { where: (q.where || []).concat([[f, op, v]]) })),
      limit: (n) => query(path, Object.assign({}, q, { limit: n })), doc: (k) => doc(path + '/' + k),
      get: () => call(Object.assign({ t: 'query', path }, q)).then(r => { const docs = r.rows.map(x => snap(path + '/' + x.id, x.data)); return { docs, size: docs.length, empty: !docs.length, forEach: (f) => docs.forEach(f) }; }) };
  }
  const dbApi = { doc, collection: (p) => query(p, {}) };
  const userApi = { id: () => Promise.resolve(uid) };
  window.claude = { use: (name) => Promise.resolve(name === 'room' ? roomApi : name === 'db' ? dbApi : name === 'user' ? userApi : null) };
})();
