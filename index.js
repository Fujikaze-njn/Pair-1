import express from 'express';
import fs from 'fs';
import path from 'path';
import pino from 'pino';
import NodeCache from 'node-cache';
import { Mutex } from 'async-mutex';
import crypto from 'crypto';
import { createClient } from '@supabase/supabase-js';
import {
    useMultiFileAuthState,
    Browsers,
    makeWASocket,
    fetchLatestBaileysVersion
} from 'baileys';
import config from './config.js';

const app = express();
const port = 3000;
const sessionDir = './session';
const msgRetryCounterCache = new NodeCache();
const mutex = new Mutex();
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true });
}

const supabase = createClient(config.DBURL, config.SUPKEY);
app.use(express.static(path.join('./static')));
app.use(express.json());

/**
 * Upload session files to Supabase
 */
async function saveToSupabase(sessionId, sessionPath) {
    try {
        const files = fs.readdirSync(sessionPath);
        const importantFiles = files.filter(f => f.startsWith('app-state-sync') || f === 'creds.json');

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
    } catch (uploadError) {
        console.error('Upload error:', uploadError);
    }
}

/**
 * Cleanup helper
 */
function cleanup(sessionId, sessionPath, session = null) {
    try {
        if (session) {
            try { session.end(); } catch {}
        }
        fs.rmSync(sessionPath, { recursive: true, force: true });
        console.log(`🧹 Cleaned up session ${sessionId}`);
    } catch (err) {
        console.warn(`Cleanup error for ${sessionId}:`, err.message);
    }
}

/**
 * Main WhatsApp connector
 */
async function connector(number, res) {
    const sessionId = `Nexus_${crypto.randomBytes(8).toString('hex')}`;
    const sessionPath = path.join(sessionDir, sessionId);

    // Auto-cleanup timer (10 minutes)
    const timeout = setTimeout(() => {
        console.log(`⏱️ Session ${sessionId} timed out`);
        cleanup(sessionId, sessionPath);
    }, 10 * 60 * 1000);

    try {
        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version, isLatest } = await fetchLatestBaileysVersion();
        console.log(`Using WA v${version.join('.')}, isLatest: ${isLatest}`);

        const session = makeWASocket({
            auth: state,
            logger: pino({ level: 'fatal' }), // quiet logs
            version,
            browser: Browsers.macOS('Safari'),
            markOnlineOnConnect: false,
            msgRetryCounterCache
        });

        // Save creds locally + Supabase immediately
        session.ev.on('creds.update', async () => {
            await saveCreds();
            await saveToSupabase(sessionId, sessionPath);
        });

        // If not registered yet, request a pairing code
        if (!state?.creds?.registered) {
            if (!number) {
                if (res && !res.headersSent) {
                    clearTimeout(timeout);
                    return res.status(400).json({ message: 'Input your number' });
                }
                return;
            }

            const cleaned = number.replace(/\D/g, '');

            try {
                // 🔑 Essential delay for valid pairing code
                await delay(1500);

                const code = await session.requestPairingCode(cleaned);
                const formattedCode = code?.match(/.{1,4}/g)?.join('-') ?? code;

                if (res && !res.headersSent) {
                    res.json({
                        code: formattedCode,
                        sessionId: sessionId,
                        message: 'Use this code to pair your device'
                    });
                }
            } catch (err) {
                clearTimeout(timeout);
                console.error('Error requesting pairing code:', err?.message ?? err);
                if (res && !res.headersSent) {
                    res.status(500).json({ error: 'Failed to generate pairing code', details: err?.message });
                }
            }
        }

        session.ev.on('connection.update', async update => {
            const { connection } = update;

            if (connection === 'open') {
                console.log('✅ Connection established for', sessionId);

                await saveToSupabase(sessionId, sessionPath);

                try {
                    if (session.user?.id) {
                        await session.sendMessage(session.user.id, {
                            text: `✅ Session ID: ${sessionId}\nKeep this safe.`
                        });
                    }
                } catch (errSend) {
                    console.warn('Could not send message to connected account:', errSend?.message ?? errSend);
                }

                // Clear timeout and cleanup
                clearTimeout(timeout);
                cleanup(sessionId, sessionPath, session);
            }
        });
    } catch (error) {
        clearTimeout(timeout);
        console.error('Session creation error:', error);
        cleanup(sessionId, sessionPath);
        if (res && !res.headersSent) {
            res.status(500).json({ error: 'Failed to create session' });
        }
    }
}

// === Routes ===

app.get('/pair', async (req, res) => {
    const number = req.query.number;

    if (!number) {
        return res.status(400).json({ message: 'Number required' });
    }

    const release = await mutex.acquire();

    try {
        await connector(number, res);
    } catch (err) {
        console.error('Pairing error:', err);
        if (!res.headersSent) {
            res.status(500).json({ error: 'Error connecting to WhatsApp' });
        }
    } finally {
        release();
    }
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        timestamp: new Date().toISOString()
    });
});

// === Start server ===
app.listen(port, () => {
    console.log(`🚀 Server running on port ${port}`);
    console.log(`🔍 Health check available at http://localhost:${port}/health`);
});