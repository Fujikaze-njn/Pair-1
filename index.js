// index.mjs
import express from "express";
import fs from "fs";
import pino from "pino";
import NodeCache from "node-cache";
import {
    default as makeWASocket,
    useMultiFileAuthState,
    delay,
    Browsers,
    makeCacheableSignalKeyStore,
    DisconnectReason
} from "baileys";
import { Mutex } from "async-mutex";
import path from "path";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";
import config from "./config.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const port = 3000;
const msgRetryCounterCache = new NodeCache();
const mutex = new Mutex();
let session;

app.use(express.static(path.join(__dirname, "static")));

// Supabase client
const supabase = createClient(config.DBURL, config.SUPKEY);

function generateSessionId() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let randomPart = "";
    for (let i = 0; i < 8; i++) {
        randomPart += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return `Nexus_${randomPart}`;
}

async function uploadSessionFiles(sessionDir, sessionId) {
    const files = fs.readdirSync(sessionDir);

    for (const file of files) {
        const filePath = path.join(sessionDir, file);
        const buffer = fs.readFileSync(filePath);

        const remotePath = `${sessionId}/${file}`;
        const { error } = await supabase.storage
            .from("session")
            .upload(remotePath, buffer, {
                cacheControl: "3600",
                upsert: true,
                contentType: "application/json"
            });

        if (error) throw error;
    }

    return sessionId; // return only the ID (not URL)
}

async function connector(Num, res) {
    const sessionDir = path.join(__dirname, "session");
    if (!fs.existsSync(sessionDir)) {
        fs.mkdirSync(sessionDir);
    }

    const { state, saveCreds } = await useMultiFileAuthState(sessionDir);

    session = makeWASocket({
        auth: {
            creds: state.creds,
            keys: makeCacheableSignalKeyStore(
                state.keys,
                pino({ level: "fatal" }).child({ level: "fatal" })
            )
        },
        logger: pino({ level: "fatal" }).child({ level: "fatal" }),
        browser: Browsers.macOS("Safari"),
        markOnlineOnConnect: true,
        msgRetryCounterCache
    });

    if (!session.authState.creds.registered) {
        await delay(1500);
        Num = Num.replace(/[^0-9]/g, "");
        const code = await session.requestPairingCode(Num);
        if (!res.headersSent) {
            res.send({ code: code?.match(/.{1,4}/g)?.join("-") });
        }
    }

    session.ev.on("creds.update", async () => {
        await saveCreds();
    });

    session.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect } = update;

    if (connection === "open") {
        console.log("Connected successfully");
        await delay(3000);
        try {
            const sessionId = generateSessionId();
            await uploadSessionFiles(sessionDir, sessionId);
            console.log("Session uploaded with ID:", sessionId);

            await session.sendMessage(session.user.id, {
                image: { url: "https://cdn.kordai.biz.id/serve/JpKYo5TCwETY.jpg" },
                caption: sessionId
            });

            if (res && !res.headersSent) {
                res.json({ sessionId });
            }
            setTimeout(() => {
                if (fs.existsSync(sessionDir)) {
                    fs.rmSync(sessionDir, { recursive: true, force: true });
                    console.log("Session folder deleted locally");
                }
            }, 5000);
        } catch (error) {
            console.error("Upload error:", error);
        }
    } else if (connection === "close") {
        const reason = lastDisconnect?.error?.output?.statusCode;
        reconn(reason);
    }
});
}

function reconn(reason) {
    if (
        [DisconnectReason.connectionLost, DisconnectReason.connectionClosed, DisconnectReason.restartRequired].includes(reason)
    ) {
        console.log("Connection lost, reconnecting...");
        connector();
    } else {
        console.log(`Disconnected! reason: ${reason}`);
        session.end();
    }
}

app.get("/pair", async (req, res) => {
    const Num = req.query.code;
    if (!Num) {
        return res.status(418).json({ message: "Phone number is required" });
    }

    const release = await mutex.acquire();
    try {
        await connector(Num, res);
    } catch (error) {
        console.log(error);
        res.status(500).json({ error: "Something went wrong" });
    } finally {
        release();
    }
});

app.listen(port, () => {
    console.log(`Running on PORT:${port}`);
});