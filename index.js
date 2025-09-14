import express from 'express';
import fs from 'fs';
import path from 'path';
import pino from 'pino';
import NodeCache from 'node-cache';
import { Mutex } from 'async-mutex';
import crypto from 'crypto';
import { Boom } from '@hapi/boom';
import { createClient } from '@supabase/supabase-js';
import {
    useMultiFileAuthState,
    Browsers,
    makeCacheableSignalKeyStore,
    DisconnectReason,
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

const activeSessions = new Map();

/**
 * Restore session files from Supabase into local sessionPath
 */
async function restoreFromSupabase(sessionId, sessionPath) {
    try {
        const { data, error } = await supabase.storage.from('session').list(sessionId);
        if (error) {
            console.warn(`Supabase list error: ${error.message}`);
            return;
        }
        if (!data || data.length === 0) return;

        fs.mkdirSync(sessionPath, { recursive: true });

        for (const file of data) {
            const { data: fileData, error: downloadError } = await supabase.storage
                .from('session')
                .download(`${sessionId}/${file.name}`);
            if (downloadError) {
                console.warn(`Download error for ${file.name}: ${downloadError.message}`);
                continue;
            }
            const buffer = Buffer.from(await fileData.arrayBuffer());
            fs.writeFileSync(path.join(sessionPath, file.name), buffer);
        }
        console.log(`Restored session ${sessionId} from Supabase`);
    } catch (err) {
        console.error('restoreFromSupabase error:', err);
    }
}

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
        console.log(`Saved session ${sessionId} to Supabase`);
    } catch (uploadError) {
        console.error('Upload error:', uploadError);
    }
}

/**
 * Main WhatsApp connector
 */
async function connector(number, res) {
    const sessionId = `Nexus_${crypto.randomBytes(8).toString('hex')}`;
    const sessionPath = path.join(sessionDir, sessionId);

    await restoreFromSupabase(sessionId, sessionPath);

    try {
        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version, isLatest } = await fetchLatestBaileysVersion();
        console.log(`Using WA v${version.join('.')}, isLatest: ${isLatest}`);

        const session = makeWASocket({
            auth: state,
            logger: pino({ level: 'info' }),
            version,
            browser: Browsers.macOS('Safari'),
            markOnlineOnConnect: true,
            msgRetryCounterCache
        });

        session.ev.on('creds.update', saveCreds);
        activeSessions.set(sessionId, { session, sessionPath });

        // If not registered yet, request a pairing code
        if (!state?.creds?.registered) {
            if (!number) {
                if (res && !res.headersSent) {
                    return res.status(400).json({ message: 'Input your number' });
                }
                return;
            }

            await delay(1500);
            const cleaned = number.replace(/\D/g, '');

            try {
                const code = await session.requestPairingCode(cleaned);
                const formattedCode = code?.match(/.{1,4}/g)?.join('-') ?? code;

                if (res && !res.headersSent) {
                    return res.json({
                        code: formattedCode,
                        sessionId: sessionId,
                        message: 'Use this code to pair your device'
                    });
                } else {
                    console.log('Pairing code:', formattedCode, 'sessionId:', sessionId);
                    return;
                }
            } catch (err) {
                console.error('Error requesting pairing code:', err?.message ?? err);
                if (res && !res.headersSent) {
                    return res.status(500).json({ error: 'Failed to generate pairing code', details: err?.message });
                }
                return;
            }
        }

        session.ev.on('connection.update', async update => {
            console.log('connection.update:', JSON.stringify(update));
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                console.log('QR string present (length):', qr.length);
            }

            if (connection === 'open') {
                console.log('Connection established successfully for', sessionId);

                await saveToSupabase(sessionId, sessionPath);

                try {
                    await session.sendMessage(session.user?.id, {
                        text: `Session ID: ${sessionId}\nKeep this safe.`
                    });
                } catch (errSend) {
                    console.warn('Could not send message to connected account:', errSend?.message ?? errSend);
                }
            } else if (connection === 'close') {
                const code = new Boom(lastDisconnect?.error).output?.statusCode;
                console.log('Connection closed, reason code:', code);

                if (code === DisconnectReason.loggedOut) {
                    try {
                        session.end();
                    } catch (e) {
                        console.warn('Error ending session:', e);
                    }
                    activeSessions.delete(sessionId);
                    try {
                        fs.rmSync(sessionPath, { recursive: true, force: true });
                    } catch (err) {
                        console.warn('Cleanup error:', err);
                    }
                } else {
                    console.log('Attempting reconnect in 5s for', sessionId);
                    setTimeout(() => {
                        connector(number, null).catch(console.error);
                    }, 5000);
                }
            }
        });
    } catch (error) {
        console.error('Session creation error:', error);
        try {
            fs.rmSync(sessionPath, { recursive: true, force: true });
        } catch (cleanupError) {
            console.warn('Cleanup error:', cleanupError);
        }

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
        activeSessions: activeSessions.size,
        timestamp: new Date().toISOString()
    });
});

app.get('/cleanup', (req, res) => {
    const sessionId = req.query.sessionId;
    if (sessionId && activeSessions.has(sessionId)) {
        const sessionData = activeSessions.get(sessionId);
        try {
            sessionData.session.end();
            fs.rmSync(sessionData.sessionPath, { recursive: true, force: true });
            activeSessions.delete(sessionId);
            res.json({ message: 'Session cleaned up' });
        } catch (error) {
            res.status(500).json({ error: 'Cleanup failed' });
        }
    } else {
        res.status(404).json({ error: 'Session not found' });
    }
});

// === Start server ===
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
    console.log(`Health check available at http://localhost:${port}/health`);
});