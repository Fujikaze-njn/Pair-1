import express from 'express';
import fs from 'fs';
import path from 'path';
import pino from 'pino';
import NodeCache from 'node-cache';
import { Mutex } from 'async-mutex';
import { createClient } from '@supabase/supabase-js';
import {
  useMultiFileAuthState,
  Browsers,
  makeWASocket,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  DisconnectReason
} from 'baileys';
import config from './config.js';

const app = express();
const port = 3000;

const sessionRoot = path.join(process.cwd(), 'session');
if (!fs.existsSync(sessionRoot)) fs.mkdirSync(sessionRoot, { recursive: true });

const msgRetryCounterCache = new NodeCache();
const mutex = new Mutex();
const delay = ms => new Promise(r => setTimeout(r, ms));

const supabase = createClient(config.DBURL, config.SUPKEY);

app.use(express.static(path.join(process.cwd(), 'static')));
app.use(express.json());

/**
 * Upload important session files to Supabase
 */
async function saveToSupabase(sessionId, sessionPath) {
  if (!fs.existsSync(sessionPath)) return;
  const files = fs.readdirSync(sessionPath);
  const importantFiles = files.filter(f =>
    f.startsWith('app-state-sync') || f === 'creds.json' || f.startsWith('session')
  );

  await Promise.all(
    importantFiles.map(async f => {
      const filePath = path.join(sessionPath, f);
      const fileContent = fs.readFileSync(filePath);
      await supabase.storage
        .from('session')
        .upload(`${sessionId}/${f}`, fileContent, {
          contentType: 'application/json',
          upsert: true
        });
    })
  );

  console.log(`✅ Saved session ${sessionId} to Supabase`);
}

/**
 * Cleanup only if logged out
 */
function cleanup(sessionId, sessionPath, socket = null) {
  try {
    if (socket) {
      try {
        socket.end();
      } catch {}
    }
    if (fs.existsSync(sessionPath)) {
      fs.rmSync(sessionPath, { recursive: true, force: true });
    }
    console.log(`🧹 Cleaned up session ${sessionId}`);
  } catch (err) {
    console.warn(`Cleanup error for ${sessionId}:`, err?.message ?? err);
  }
}

/**
 * Connector function
 */
async function connector(number, res) {
  const cleaned = number.replace(/\D/g, '');
  const sessionId = `Nexus_${cleaned}`;
  const sessionPath = path.join(sessionRoot, sessionId);
  if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

  let socket = null;
  try {
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();
    console.log(`⚡ Using WA v${version.join('.')}`);

    socket = makeWASocket({
      auth: {
        creds: state.creds,
        keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }))
      },
      logger: pino({ level: 'fatal' }),
      version,
      browser: Browsers.macOS('Safari'),
      markOnlineOnConnect: true,
      msgRetryCounterCache
    });

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
      } catch (e) {
        console.warn('⚠️ saveCreds failed', e?.message ?? e);
      }
    });

    // Pairing if not registered
    if (!state?.creds?.registered) {
      try {
        await delay(1500);
        const code = await socket.requestPairingCode(cleaned);
        const formatted = code?.match(/.{1,4}/g)?.join('-') ?? code;
        if (res && !res.headersSent) {
          return res.json({
            code: formatted,
            sessionId,
            message: 'Use this code to pair your device'
          });
        }
      } catch (err) {
        console.error('❌ Error requesting pairing code:', err?.message ?? err);
        if (res && !res.headersSent) {
          res.status(500).json({ error: 'Failed to generate pairing code', details: err?.message });
        }
        cleanup(sessionId, sessionPath, socket);
        return;
      }
    }

    // Connection updates
    socket.ev.on('connection.update', async update => {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        console.log(`✅ Connected for ${sessionId}`);

        // Upload in background
        saveToSupabase(sessionId, sessionPath).catch(e =>
          console.warn('Upload failed:', e?.message ?? e)
        );

        if (res && !res.headersSent) {
          res.json({ sessionId, message: 'Device connected and session saved' });
        }
      }

      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        console.log(`⚠️ Connection closed for ${sessionId}. Reason:`, reason);

        if (
          [DisconnectReason.connectionLost, DisconnectReason.connectionClosed, DisconnectReason.restartRequired].includes(reason)
        ) {
          console.log('🔄 Reconnecting...');
          connector(number, null);
        } else if (reason === DisconnectReason.loggedOut) {
          cleanup(sessionId, sessionPath, socket);
        }
      }
    });
  } catch (err) {
    console.error('❌ Session creation error:', err?.message ?? err);
    cleanup(sessionId, sessionPath, socket);
    if (res && !res.headersSent) {
      res.status(500).json({ error: 'Failed to create session' });
    }
  }
}

/**
 * Routes
 */
app.get('/pair', async (req, res) => {
  const number = req.query.number || req.query.code;
  if (!number) {
    return res.status(400).json({ message: 'Number required' });
  }

  const release = await mutex.acquire();
  try {
    await connector(number, res);
  } catch (err) {
    console.error('Pairing error:', err?.message ?? err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Error connecting to WhatsApp' });
    }
  } finally {
    release();
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});