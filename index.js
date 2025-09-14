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
const port = 7860;
const sessionDir = './session';
const msgRetryCounterCache = new NodeCache();
const mutex = new Mutex();

// Ensure session directory exists
if (!fs.existsSync(sessionDir)) {
    fs.mkdirSync(sessionDir, { recursive: true });
}

const supabase = createClient(config.DBURL, config.SUPKEY);
app.use(express.static(path.join('./static')));
app.use(express.json());

// Store active sessions
const activeSessions = new Map();

async function connector(number, res) {
    const sessionId = `Nexus_${crypto.randomBytes(8).toString('hex')}`;
    const sessionPath = path.join(sessionDir, sessionId);
    
    if (!fs.existsSync(sessionPath)) {
        fs.mkdirSync(sessionPath, { recursive: true });
    }

    try {
        const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
        const { version, isLatest } = await fetchLatestBaileysVersion();
        console.log(`Using WA v${version.join('.')}, isLatest: ${isLatest}`);
        
        const session = makeWASocket({
            auth: { 
                creds: state.creds, 
                keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'fatal' }).child({ level: 'fatal' })) 
            },
            logger: pino({ level: 'fatal' }).child({ level: 'fatal' }),
            version,
            browser: Browsers.macOS('Safari'),
            markOnlineOnConnect: true,
            msgRetryCounterCache
        });

        // Store session reference
        activeSessions.set(sessionId, { session, sessionPath });

        session.ev.on('creds.update', saveCreds);

        if (!session.authState.creds.registered) {
            if (!number) {
                return res.status(400).json({ message: 'Input your number' });
            }
            
            const cleaned = number.replace(/\D/g, '');
            
            try {
                const code = await session.requestPairingCode(cleaned);
                const formattedCode = code?.match(/.{1,4}/g)?.join('-');
                
                return res.json({ 
                    code: formattedCode, 
                    sessionId: sessionId,
                    message: 'Use this code to pair your device'
                });
            } catch (error) {
                console.error('Error requesting pairing code:', error);
                return res.status(500).json({ error: 'Failed to generate pairing code' });
            }
        }

        // 🔥 Single unified connection.update listener
        session.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                console.log('QR code generated:', qr);
            }

            if (connection === 'connecting') {
                console.log('Connecting...');
            }

            if (connection === 'open') {
                console.log('Connection established successfully');
                
                try {
                    const files = fs.readdirSync(sessionPath);
                    const importantFiles = files.filter(f => f.startsWith('app-state-sync') || f === 'creds.json');

                    await Promise.all(importantFiles.map(async (f) => {
                        const filePath = path.join(sessionPath, f);
                        const fileContent = fs.readFileSync(filePath);
                        
                        await supabase.storage
                            .from('session')
                            .upload(`${sessionId}/${f}`, fileContent, { 
                                contentType: 'application/json', 
                                upsert: true 
                            });
                    }));

                    // Send session ID to user
                    try {
                        await session.sendMessage(session.user.id, { 
                            text: `Session ID: ${sessionId}\nKeep this safe for future use.` 
                        });
                    } catch (sendError) {
                        console.warn('Could not send message to user:', sendError);
                    }

                    // Cleanup local files
                    importantFiles.forEach(f => {
                        try {
                            fs.unlinkSync(path.join(sessionPath, f));
                        } catch (unlinkError) {
                            console.warn('Could not delete file:', unlinkError);
                        }
                    });

                } catch (uploadError) {
                    console.error('Upload error:', uploadError);
                }

            } else if (connection === 'close') {
                const reason = new Boom(lastDisconnect?.error)?.output?.statusCode;
                console.log('Connection closed with reason:', reason);
                
                if (reason === DisconnectReason.loggedOut) {
                    try { 
                        session.end(); 
                    } catch (endError) {
                        console.warn('Error ending session:', endError);
                    }
                } else {
                    // Attempt to reconnect
                    setTimeout(() => {
                        connector(number, null).catch(console.error);
                    }, 5000);
                }
                
                // Cleanup
                activeSessions.delete(sessionId);
                try {
                    fs.rmSync(sessionPath, { recursive: true, force: true });
                } catch (cleanupError) {
                    console.warn('Cleanup error:', cleanupError);
                }
            }
        });

    } catch (error) {
        console.error('Session creation error:', error);
        // Cleanup on error
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

// Add health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: 'ok', 
        activeSessions: activeSessions.size,
        timestamp: new Date().toISOString()
    });
});

// Add cleanup endpoint
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

app.listen(port, () => {
    console.log(`Server running on port ${port}`);
    console.log(`Health check available at http://localhost:${port}/health`);
});