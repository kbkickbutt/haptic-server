const WebSocket = require('ws');
const express = require('express');
const admin = require('firebase-admin');

// Initialize Firebase Admin
const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8080;

// HTTP endpoint to send FCM
app.post('/send-haptic', async (req, res) => {
  const { fcmToken, patternType, events } = req.body;

  if (!fcmToken || !patternType) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    // Build notification based on pattern type
    const titles = {
      missing: '💜 Missing You',
      sos: '🆘 SOS!',
      custom: '💜 Touch received'
    };
    const bodies = {
      missing: 'Your partner is thinking of you',
      sos: 'Your partner needs you urgently!',
      custom: 'Your partner sent you a pattern'
    };

    const message = {
      token: fcmToken,
      data: {
        patternType: patternType,
        events: JSON.stringify(events || [])
      },
      notification: {
        title: titles[patternType] || '💜 Touch received',
        body: bodies[patternType] || 'Your partner sent you a pattern'
      },
      android: {
        priority: 'high',
        notification: {
          channelId: 'haptic_receive_channel',
          priority: 'high'
        }
      }
    };

    const response = await admin.messaging().send(message);
    console.log('FCM sent:', response);
    res.json({ success: true, messageId: response });

  } catch (error) {
    console.error('FCM error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'LDR Haptic server running 💜' });
});

// WebSocket server
const server = app.listen(PORT, () => {
  console.log(`LDR Haptic server running on port ${PORT}`);
});

const wss = new WebSocket.Server({ server });
const rooms = {};

wss.on('connection', (ws) => {
  let myRoom = null;
  let myRole = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || !msg.type) return;

    if (msg.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
    }
  });

  ws.on('close', () => {
    if (myRoom && rooms[myRoom]) {
      delete rooms[myRoom][myRole];
      if (Object.keys(rooms[myRoom]).length === 0) delete rooms[myRoom];
    }
  });

  ws.on('error', () => { try { ws.close(); } catch {} });
});
