const WebSocket = require('ws');

const wss = new WebSocket.Server({ 
  port: process.env.PORT || 8080,
  maxPayload: 64 * 1024 // 64KB max message size
});

const rooms = {};
const ipConnections = {};
const MAX_CONNECTIONS_PER_IP = 5;
const ROOM_EXPIRY_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_EVENTS_PER_PATTERN = 500;
const MAX_MESSAGE_RATE = 10; // max 10 messages per second

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({ length: 6 }, () => 
    chars[Math.floor(Math.random() * chars.length)]).join('');
}

function isValidCode(code) {
  return typeof code === 'string' && /^[A-Z2-9]{6}$/.test(code);
}

function isValidHapticData(events) {
  if (!Array.isArray(events)) return false;
  if (events.length > MAX_EVENTS_PER_PATTERN) return false;
  return events.every(e =>
    typeof e.dt === 'number' && e.dt >= 0 && e.dt <= 30000 &&
    typeof e.x === 'number' && e.x >= 0 && e.x <= 1 &&
    typeof e.y === 'number' && e.y >= 0 && e.y <= 1 &&
    [0, 1, 2].includes(e.action)
  );
}

function cleanupRooms() {
  const now = Date.now();
  for (const code in rooms) {
    if (now - rooms[code].createdAt > ROOM_EXPIRY_MS) {
      for (const ws of Object.values(rooms[code].members)) {
        try { ws.close(1000, 'Room expired'); } catch {}
      }
      delete rooms[code];
      console.log(`[${code}] Room expired and cleaned up`);
    }
  }
}

// Clean up old rooms every hour
setInterval(cleanupRooms, 60 * 60 * 1000);

wss.on('connection', (ws, req) => {
  const ip = req.headers['x-forwarded-for']?.split(',')[0].trim() 
             || req.socket.remoteAddress;

  // Rate limit connections per IP
  ipConnections[ip] = (ipConnections[ip] || 0) + 1;
  if (ipConnections[ip] > MAX_CONNECTIONS_PER_IP) {
    ws.close(1008, 'Too many connections');
    return;
  }

  let myRoom = null;
  let myRole = null;
  let lastMessageTime = Date.now();
  let messageCount = 0;
  let messageRateTimer = null;

  // Reset message rate counter every second
  messageRateTimer = setInterval(() => { messageCount = 0; }, 1000);

  ws.on('message', (raw) => {
    // Rate limiting
    messageCount++;
    if (messageCount > MAX_MESSAGE_RATE) {
      ws.close(1008, 'Rate limit exceeded');
      return;
    }

    // Parse and validate JSON
    let msg;
    try {
      const text = raw.toString();
      if (text.length > 32768) { ws.close(1009, 'Message too large'); return; }
      msg = JSON.parse(text);
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid data' }));
      return;
    }

    if (!msg || typeof msg !== 'object' || !msg.type) return;

    if (msg.type === 'join') {
      const code = String(msg.code || '').toUpperCase().trim();

      if (!isValidCode(code)) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid room code' }));
        return;
      }

      if (!rooms[code]) {
        rooms[code] = { members: {}, createdAt: Date.now() };
      }

      const taken = Object.keys(rooms[code].members);
      if (taken.length >= 2) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room is full' }));
        return;
      }

      myRole = taken.length === 0 ? 'a' : 'b';
      myRoom = code;
      rooms[code].members[myRole] = ws;

      ws.send(JSON.stringify({ type: 'joined', role: myRole, code }));

      // Notify partner
      for (const [role, sock] of Object.entries(rooms[code].members)) {
        if (role !== myRole && sock.readyState === WebSocket.OPEN) {
          sock.send(JSON.stringify({ type: 'partner_joined' }));
        }
      }
      console.log(`[${code}] ${myRole} joined`);
    }

    else if (msg.type === 'haptic') {
      if (!myRoom || !rooms[myRoom]) return;

      // Validate haptic data strictly
      if (!isValidHapticData(msg.events)) {
        ws.send(JSON.stringify({ type: 'error', message: 'Invalid haptic data' }));
        return;
      }

      // Only send to partner, never echo back
      const sanitized = JSON.stringify({
        type: 'haptic',
        events: msg.events.map(e => ({
          dt: Math.round(Number(e.dt)),
          x: Math.min(1, Math.max(0, Number(e.x))),
          y: Math.min(1, Math.max(0, Number(e.y))),
          action: Number(e.action)
        }))
      });

      for (const [role, sock] of Object.entries(rooms[myRoom].members)) {
        if (role !== myRole && sock.readyState === WebSocket.OPEN) {
          sock.send(sanitized);
        }
      }
    }
  });

  ws.on('close', () => {
    clearInterval(messageRateTimer);
    ipConnections[ip] = Math.max(0, (ipConnections[ip] || 1) - 1);

    if (myRoom && rooms[myRoom]) {
      delete rooms[myRoom].members[myRole];
      for (const sock of Object.values(rooms[myRoom].members)) {
        if (sock.readyState === WebSocket.OPEN) {
          sock.send(JSON.stringify({ type: 'partner_left' }));
        }
      }
      if (Object.keys(rooms[myRoom].members).length === 0) {
        delete rooms[myRoom];
        console.log(`[${myRoom}] Room deleted`);
      }
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    try { ws.close(); } catch {}
  });
});

console.log('Secure haptic server running on port', process.env.PORT || 8080);
