const WebSocket = require('ws');
const wss = new WebSocket.Server({ port: process.env.PORT || 8080 });

// rooms: { "ABCD12": { a: ws, b: ws } }
const rooms = {};

function broadcast(room, senderWs, data) {
  for (const ws of Object.values(room)) {
    if (ws !== senderWs && ws.readyState === WebSocket.OPEN) {
      ws.send(data);
    }
  }
}

wss.on('connection', (ws) => {
  let myRoom = null;
  let myRole = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const code = msg.code.toUpperCase();
      if (!rooms[code]) rooms[code] = {};
      const taken = Object.keys(rooms[code]);

      if (taken.length >= 2) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room full' }));
        return;
      }

      myRole = taken.length === 0 ? 'a' : 'b';
      myRoom = code;
      rooms[code][myRole] = ws;

      ws.send(JSON.stringify({ type: 'joined', role: myRole, code }));

      // Notify partner if already present
      broadcast(rooms[code], ws, JSON.stringify({ type: 'partner_joined' }));
      console.log(`[${code}] ${myRole} joined`);
    }

    if (msg.type === 'haptic' && myRoom) {
      // Relay the touch pattern to the other partner
      broadcast(rooms[myRoom], ws, JSON.stringify(msg));
    }
  });

  ws.on('close', () => {
    if (myRoom && rooms[myRoom]) {
      delete rooms[myRoom][myRole];
      broadcast(rooms[myRoom], ws, JSON.stringify({ type: 'partner_left' }));
      if (Object.keys(rooms[myRoom]).length === 0) delete rooms[myRoom];
      console.log(`[${myRoom}] ${myRole} left`);
    }
  });
});

console.log('Haptic server running on port', process.env.PORT || 8080);