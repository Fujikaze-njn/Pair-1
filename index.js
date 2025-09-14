import express from 'express';
import fs from 'fs';
import path from 'path';
import pino from 'pino';
import NodeCache from 'node-cache';
import { Mutex } from 'async-mutex';
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';
import { useMultiFileAuthState, Browsers, makeWASocket, fetchLatestBaileysVersion, makeCacheableSignalKeyStore, DisconnectReason } from 'baileys';
import config from './config.js';

const app = express();
const port = 3000;
const sessionDir = path.join(process.cwd(), 'session');
const msgRetryCounterCache = new NodeCache();
const mutex = new Mutex();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

if (!fs.existsSync(sessionDir)) fs.mkdirSync(sessionDir, { recursive: true });

const supabase = createClient(config.DBURL, config.SUPKEY);
app.use(express.static(path.join(process.cwd(), 'static')));
app.use(express.json());

async function saveToSupabase(sessionId, sessionPath) {
  if (!fs.existsSync(sessionPath)) return;
  const files = fs.readdirSync(sessionPath);
  const importantFiles = files.filter(f => f.startsWith('app-state-sync') || f === 'creds.json' || f.startsWith('session'));
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
  console.log(`Saved session ${sessionId} to Supabase`);
}

function cleanup(sessionId, sessionPath, session = null) {
  try {
    if (session) {
      try {
        session.end();
      } catch {}
    }
    if (fs.existsSync(sessionPath)) {
      fs.rmSync(sessionPath, { recursive: true, force: true });
    }
    console.log(`Cleaned up session ${sessionId}`);
  } catch (err) {
    console.warn(`Cleanup error for ${sessionId}:`, err?.message ?? err);
  }
}

async function connector(number, res) {
  const sessionId = `Nexus_${crypto.randomBytes(8).toString('hex')}`;
  const sessionPath = path.join(sessionDir, sessionId);
  if (!fs.existsSync(sessionPath)) fs.mkdirSync(sessionPath, { recursive: true });

  let baileysSession = null;
  try {
    const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
    const { version } = await fetchLatestBaileysVersion();
    console.log(`Using WA v${version.join('.')}`);

    baileysSession = makeWASocket({
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

    baileysSession.ev.on('creds.update', async () => {
      try {
        await saveCreds();
      } catch (e) {
        console.warn('saveCreds failed', e?.message ?? e);
      }
    });

    if (!state?.creds?.registered) {
      if (!number) {
        if (res && !res.headersSent) return res.status(400).json({ message: 'Phone number required' });
        return;
      }
      const cleaned = number.replace(/\D/g, '');
      try {
        await delay(1500);
        const code = await baileysSession.requestPairingCode(cleaned);
        const formattedCode = code?.match(/.{1,4}/g)?.join('-') ?? code;
        if (res && !res.headersSent) {
          res.json({ code: formattedCode, sessionId, message: 'Use this code to pair your device' });
        }
      } catch (err) {
        console.error('Error requesting pairing code:', err?.message ?? err);
        if (res && !res.headersSent) {
          res.status(500).json({ error: 'Failed to generate pairing code', details: err?.message });
        }
        cleanup(sessionId, sessionPath, baileysSession);
        return;
      }
    }

    baileysSession.ev.on('connection.update', async update => {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        console.log('Connected for', sessionId);
        try {
          await saveToSupabase(sessionId, sessionPath);
          console.log(`Uploaded session ${sessionId}`);
          const jid = baileysSession.user?.id;
          if (jid) {
            await baileysSession.sendMessage(jid, { text: `Session established: ${sessionId}` });
          }
          if (res && !res.headersSent) {
            res.json({ sessionId, message: 'Device connected and session uploaded' });
          }
        } catch (errUpload) {
          console.warn('Upload failed:', errUpload?.message ?? errUpload);
        }
        await delay(5000);
        cleanup(sessionId, sessionPath, baileysSession);
      }

      if (connection === 'close') {
        const reason = lastDisconnect?.error?.output?.statusCode;
        console.log(`Connection closed for ${sessionId}. Reason:`, reason);
        if (reason === DisconnectReason.loggedOut) {
          cleanup(sessionId, sessionPath, baileysSession);
        }
      }
    });
  } catch (error) {
    console.error('Session creation error:', error?.message ?? error);
    cleanup(sessionId, sessionPath, baileysSession);
    if (res && !res.headersSent) {
      res.status(500).json({ error: 'Failed to create session' });
    }
  }
}

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
  console.log(`Server running on port ${port}`);
});