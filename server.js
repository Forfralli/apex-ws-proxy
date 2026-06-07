/**
 * apex-ws-proxy — server.js
 * Proxy WebSocket (Apex Timing) → SSE (browser)
 * Deploy su Render: Start Command = "node server.js"
 */

const http      = require('http');
const WebSocket = require('ws');

const PORT    = process.env.PORT || 3000;
const WS_URL  = 'wss://live-data.apex-timing.com:8863/';
const WS_PAGE = 'https://live.apex-timing.com/vm-karting/';
const ORIGIN  = 'https://live.apex-timing.com';

/* ── Clienti SSE connessi ─────────────────────────── */
const clients = new Set();

/* ── Cache dell'ultimo init ricevuto ─────────────── */
/* Quando un nuovo browser si connette a metà sessione,
   gli mandiamo subito l'init così conosce nomi e stato */
let lastInit = null;

/* ── Connessione WebSocket verso Apex Timing ─────── */
let apexWs    = null;
let reconnTimer = null;

function connectApex() {
  console.log('[apex] Connessione a', WS_URL);

  apexWs = new WebSocket(WS_URL, {
    headers: { Origin: ORIGIN }
  });

  apexWs.on('open', () => {
    console.log('[apex] Connesso — invio URL pagina');
    apexWs.send(WS_PAGE);
    broadcast('connected', JSON.stringify({ msg: 'WebSocket Apex connesso' }));
  });

  apexWs.on('message', (data) => {
    const msg = data.toString();

    /* salva l'init più recente */
    if (msg.startsWith('init|p|')) {
      lastInit = msg;
      console.log('[apex] Init ricevuto e cachato');
    }

    broadcast(null, JSON.stringify(msg));
  });

  apexWs.on('close', (code) => {
    console.log(`[apex] Chiuso (${code}) — riconnessione in 4s`);
    broadcast('closed', JSON.stringify({ msg: 'Disconnesso, riconnessione...' }));
    clearTimeout(reconnTimer);
    reconnTimer = setTimeout(connectApex, 4000);
  });

  apexWs.on('error', (err) => {
    console.error('[apex] Errore:', err.message);
    broadcast('error', JSON.stringify({ msg: err.message }));
  });
}

/* ── Broadcast a tutti i client SSE ─────────────── */
function broadcast(event, data) {
  for (const res of clients) {
    try {
      sendToClient(res, event, data);
    } catch (e) {
      clients.delete(res);
    }
  }
}

function sendToClient(res, event, data) {
  if (event) {
    res.write(`event: ${event}\ndata: ${data}\n\n`);
  } else {
    res.write(`data: ${data}\n\n`);
  }
}

/* ── HTTP Server ─────────────────────────────────── */
const server = http.createServer((req, res) => {

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  /* Health check */
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }

  /* SSE endpoint: GET /stream */
  if (req.url === '/stream') {
    res.writeHead(200, {
      'Content-Type':      'text/event-stream',
      'Cache-Control':     'no-cache',
      'Connection':        'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.write(': connesso\n\n');

    /* manda subito l'init cachato se disponibile —
       il browser riceve nomi e posizioni anche a sessione in corso */
    if (lastInit) {
      console.log('[sse] Invio init cachato al nuovo client');
      sendToClient(res, null, JSON.stringify(lastInit));
    }

    clients.add(res);
    console.log(`[sse] +client (tot: ${clients.size})`);

    /* heartbeat ogni 25s */
    const hb = setInterval(() => {
      try { res.write(': heartbeat\n\n'); } catch { clearInterval(hb); }
    }, 25000);

    req.on('close', () => {
      clearInterval(hb);
      clients.delete(res);
      console.log(`[sse] -client (tot: ${clients.size})`);
    });

    return;
  }

  res.writeHead(404);
  res.end('Not found — usa /stream per SSE');
});

server.listen(PORT, () => {
  console.log(`[server] In ascolto su porta ${PORT}`);
  connectApex();
});
