import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = process.env.PORT || 8080;
const ROOM_KEY = process.env.ROOM_KEY;
if (!ROOM_KEY) {
  console.error('Set the ROOM_KEY environment variable');
  process.exit(1);
}

// Plain HTTP endpoint so the host can check the server is alive
const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end('ok');
});
const wss = new WebSocketServer({ server, maxPayload: 50 * 1024 * 1024 });

const clients = new Map();       // deviceId -> { ws, role, name }
const linkByPhone = new Map();   // phoneId -> laptopId
const linkByLaptop = new Map();  // laptopId -> phoneId

const send = (ws, msg) => {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
};
const sendTo = (id, msg) => {
  const c = clients.get(id);
  if (c) send(c.ws, msg);
};
const phoneList = () =>
  [...clients.entries()]
    .filter(([, c]) => c.role === 'phone')
    .map(([id, c]) => ({ id, name: c.name }));
const broadcastPhones = () => {
  for (const [, c] of clients) {
    if (c.role === 'laptop') send(c.ws, { type: 'phones', phones: phoneList() });
  }
};

wss.on('connection', (ws) => {
  let me = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // Register: must carry the correct key
    if (msg.type === 'register') {
      if (msg.key !== ROOM_KEY) {
        send(ws, { type: 'error', error: 'bad key' });
        return ws.close();
      }
      me = msg.deviceId;
      clients.set(me, { ws, role: msg.role, name: msg.name || 'Device' });
      if (msg.role === 'phone') broadcastPhones();
      else send(ws, { type: 'phones', phones: phoneList() });
      return;
    }
    if (!me) return;

    if (msg.type === 'link') {
      sendTo(msg.phoneId, { type: 'link-request', laptopId: me, name: clients.get(me)?.name });
    }
    if (msg.type === 'link-confirm') {
      if (!clients.has(msg.laptopId)) return;
      linkByPhone.set(me, msg.laptopId);
      linkByLaptop.set(msg.laptopId, me);
      sendTo(msg.laptopId, { type: 'linked', phoneId: me, name: clients.get(me)?.name });
    }
    if (msg.type === 'link-deny') {
      sendTo(msg.laptopId, { type: 'link-denied' });
    }
    if (msg.type === 'request') {
      const phoneId = linkByLaptop.get(me);
      if (phoneId) sendTo(phoneId, { type: 'request', id: msg.id, action: msg.action, payload: msg.payload });
    }
    if (msg.type === 'response') {
      const laptopId = linkByPhone.get(me);
      if (laptopId) sendTo(laptopId, { type: 'response', id: msg.id, data: msg.data, error: msg.error });
    }
  });

  ws.on('close', () => {
    if (!me) return;
    const info = clients.get(me);
    clients.delete(me);
    if (info?.role === 'phone') {
      const laptopId = linkByPhone.get(me);
      if (laptopId) {
        linkByPhone.delete(me);
        linkByLaptop.delete(laptopId);
        sendTo(laptopId, { type: 'unlinked' });
      }
      broadcastPhones();
    } else {
      const phoneId = linkByLaptop.get(me);
      if (phoneId) {
        linkByLaptop.delete(me);
        linkByPhone.delete(phoneId);
        sendTo(phoneId, { type: 'unlinked' });
      }
    }
  });
});

// Ping every 30s so idle connections are not dropped and dead ones are cleaned up
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => console.log(`Relay listening on port ${PORT}`));