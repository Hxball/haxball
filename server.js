
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const PORT = process.env.PORT || 3000;
const rooms = new Map();

app.use(express.static(__dirname));

app.get("/health", (_req, res) => {
  res.json({ ok: true, rooms: rooms.size });
});

function send(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function peersIn(room) {
  return [...room.clients]
    .filter(client => client.readyState === WebSocket.OPEN)
    .map(client => ({
      peer: client.peerId,
      presence: client.presence || {}
    }));
}

function broadcast(room, data, except = null) {
  for (const client of room.clients) {
    if (client !== except) send(client, data);
  }
}

function removeClient(ws) {
  const room = ws.room;
  if (!room) return;

  room.clients.delete(ws);
  broadcast(room, { t: "l", peer: ws.peerId });

  if (room.clients.size === 0) {
    rooms.delete(room.name);
  }

  ws.room = null;
}

server.on("upgrade", (req, socket, head) => {
  let url;

  try {
    url = new URL(req.url, "http://localhost");
  } catch {
    socket.destroy();
    return;
  }

  if (url.pathname !== "/ws") {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit("connection", ws, req, url);
  });
});

wss.on("connection", (ws, req, url) => {
  const roomName = (url.searchParams.get("room") || "").slice(0, 40);

  if (!/^[a-zA-Z0-9_-]{1,40}$/.test(roomName)) {
    ws.close(1008, "Invalid room");
    return;
  }

  let room = rooms.get(roomName);

  if (!room) {
    room = {
      name: roomName,
      clients: new Set()
    };
    rooms.set(roomName, room);
  }

  ws.peerId = crypto.randomUUID();
  ws.presence = {};
  ws.room = room;

  const existingPeers = peersIn(room);
  room.clients.add(ws);

  send(ws, {
    t: "hi",
    id: ws.peerId,
    peers: existingPeers
  });

  broadcast(room, {
    t: "j",
    peer: ws.peerId
  }, ws);

  ws.on("message", raw => {
    if (raw.length > 16 * 1024) {
      ws.close(1009, "Message too large");
      return;
    }

    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (
      message &&
      message.t === "p" &&
      message.d &&
      typeof message.d === "object" &&
      !Array.isArray(message.d)
    ) {
      for (const [key, value] of Object.entries(message.d)) {
        if (
          key !== "__proto__" &&
          key !== "constructor" &&
          key !== "prototype"
        ) {
          ws.presence[key] = value;
        }
      }

      broadcast(room, {
        t: "u",
        peer: ws.peerId,
        d: message.d
      }, ws);
    }
  });

  ws.on("close", () => removeClient(ws));
  ws.on("error", () => removeClient(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Servidor iniciado en http://localhost:${PORT}`);
});
