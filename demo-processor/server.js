import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { createClient } from '@supabase/supabase-js';
import { parseEvent, parseHeader } from '@laihoe/demoparser2';
import { Decompress as FzstdStreamDecompress } from 'fzstd';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { Writable } from 'node:stream';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const PORT = Number(process.env.PORT) || 10000;
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://wuhhcamvnuvkgcobrjup.supabase.co';
const SUPABASE_ANON_KEY =
  process.env.SUPABASE_ANON_KEY ||
  process.env.SUPABASE_PUBLISHABLE_KEY ||
  'sb_publishable_M-Dlo8GKZ4TV_NwcPW8XuQ_7AyVBP5n';

const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_BYTES) || 350 * 1024 * 1024; // 350 MB
const MAX_UNPACKED_BYTES = Number(process.env.MAX_UNPACKED_BYTES) || 1200 * 1024 * 1024; // 1.2 GB Ð½Ð° Ð´Ð¸ÑÐºÐµ
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS) || 3 * 60 * 1000; // 3 Ð¼Ð¸Ð½ Ð½Ð° upload
const JOB_TTL_MS = Number(process.env.JOB_TTL_MS) || 60 * 60 * 1000; // 1 ÑÐ°Ñ ÑÑÐ°Ð½ÐµÐ½Ð¸Ñ ÑÑÐ°ÑÑÑÐ° Ð² Ð¿Ð°Ð¼ÑÑÐ¸

const ALLOWED_ORIGINS = new Set([
  'https://famous-pothos-ee0313.netlify.app',
  'http://localhost:3000',
  'http://localhost:5173',
  'http://127.0.0.1:5500'
]);

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: false,
    autoRefreshToken: false
  }
});

const tempUploadDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cs2-demo-'));

const ts = () => new Date().toISOString();

const logMem = () => {
  const m = process.memoryUsage();
  return {
    rssMB: Math.round(m.rss / (1024 * 1024)),
    heapUsedMB: Math.round(m.heapUsed / (1024 * 1024)),
    externalMB: Math.round(m.external / (1024 * 1024))
  };
};

const logInfo = (stage, message, extra = {}) => {
  const payload = {
    time: ts(),
    stage,
    message,
    mem: logMem(),
    ...extra
  };
  console.log(
    `[${payload.time}][CS2-SERVER][${stage}] ${message}`,
    JSON.stringify({ mem: payload.mem, ...extra })
  );
};

const logError = (stage, err, extra = {}) => {
  const details = {
    time: ts(),
    stage,
    message: err?.message || String(err),
    code: err?.code,
    status: err?.status,
    stack: err?.stack,
    mem: logMem(),
    ...extra
  };
  console.error(`[${details.time}][CS2-SERVER][${stage}][ERROR]`, JSON.stringify(details, null, 2));
};

const cleanupFiles = async (filePaths, reqId = null) => {
  for (const filePath of filePaths) {
    if (!filePath) continue;
    try {
      await fs.promises.unlink(filePath);
      logInfo('cleanup', `Removed temp file: ${path.basename(filePath)}`, { reqId, filePath });
    } catch (unlinkErr) {
      if (unlinkErr?.code !== 'ENOENT') {
        logError('cleanup', unlinkErr, { reqId, filePath });
      }
    }
  }
};

const isAllowedOrigin = origin => {
  if (!origin) return true;
  if (ALLOWED_ORIGINS.has(origin)) return true;
  try {
    const parsed = new URL(origin);
    if (parsed.hostname.endsWith('.netlify.app') && parsed.hostname.includes('famous-pothos-ee0313')) {
      return true;
    }
  } catch {
    return false;
  }
  return false;
};

const corsOptions = {
  origin(origin, callback) {
    if (isAllowedOrigin(origin)) {
      callback(null, true);
    } else {
      logError('cors', new Error(`Origin ${origin} is not allowed by CORS policy`), { origin });
      callback(new Error(`Origin ${origin} is not allowed by CORS policy`));
    }
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Authorization', 'Content-Type', 'X-Requested-With'],
  exposedHeaders: ['Content-Length'],
  credentials: false,
  maxAge: 86400,
  optionsSuccessStatus: 204
};

const app = express();
app.disable('x-powered-by');

app.use((req, res, next) => {
  const reqId = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  req.reqId = reqId;
  const origin = req.headers.origin || 'none';
  const contentLength = req.headers['content-length'] || 'unknown';

  logInfo('http-request', `Incoming ${req.method} ${req.originalUrl} from Origin: ${origin}`, {
    reqId,
    method: req.method,
    url: req.originalUrl,
    origin,
    contentLength
  });

  req.on('timeout', () => {
    logError('http-request-timeout', new Error('Request socket/stream timed out'), {
      reqId,
      method: req.method,
      url: req.originalUrl,
      origin
    });
  });

  res.on('timeout', () => {
    logError('http-response-timeout', new Error('Response timed out'), {
      reqId,
      method: req.method,
      url: req.originalUrl,
      origin
    });
  });

  res.on('close', () => {
    if (!res.writableFinished) {
      logError('http-connection-closed', new Error('Client connection closed before response finished'), {
        reqId,
        method: req.method,
        url: req.originalUrl,
        origin
      });
    }
  });

  next();
});

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (origin && isAllowedOrigin(origin)) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-Requested-With');
  if (req.method === 'OPTIONS') {
    logInfo('http-options', `Preflight OPTIONS handled for ${req.originalUrl}`, {
      reqId: req.reqId,
      origin: req.headers.origin || 'none'
    });
    return res.status(204).end();
  }
  next();
});

const requireSupabaseAuth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization || '';
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    const token = match ? match[1].trim() : '';

    if (!token) {
      logError('auth', new Error('Missing Bearer access_token'), {
        reqId: req.reqId,
        path: req.originalUrl
      });
      return res.status(401).json({
        message: 'Ð¢ÑÐµÐ±ÑÐµÑÑÑ Ð°Ð²ÑÐ¾ÑÐ¸Ð·Ð°ÑÐ¸Ñ: Ð¾ÑÑÑÑÑÑÐ²ÑÐµÑ Bearer access_token Ð² Ð·Ð°Ð³Ð¾Ð»Ð¾Ð²ÐºÐµ Authorization.'
      });
    }

    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user) {
      logError('auth', error || new Error('Invalid or expired token'), {
        reqId: req.reqId,
        path: req.originalUrl
      });
      return res.status(401).json({
        message: 'ÐÐµÐ´ÐµÐ¹ÑÑÐ²Ð¸ÑÐµÐ»ÑÐ½ÑÐ¹ Ð¸Ð»Ð¸ Ð¸ÑÑÑÐºÑÐ¸Ð¹ ÑÐ¾ÐºÐµÐ½ Ð°Ð²ÑÐ¾ÑÐ¸Ð·Ð°ÑÐ¸Ð¸ Supabase.'
      });
    }

    req.supabaseUser = data.user;
    req.accessToken = token;
    logInfo('auth', `Supabase user authenticated: ${data.user.id}`, {
      reqId: req.reqId,
      userId: data.user.id
    });
    next();
  } catch (err) {
    logError('auth-middleware', err, { reqId: req.reqId });
    return res.status(500).json({
      message: 'ÐÐ½ÑÑÑÐµÐ½Ð½ÑÑ Ð¾ÑÐ¸Ð±ÐºÐ° Ð¿ÑÐ¸ Ð¿ÑÐ¾Ð²ÐµÑÐºÐµ Ð°Ð²ÑÐ¾ÑÐ¸Ð·Ð°ÑÐ¸Ð¸ Supabase.'
    });
  }
};

const jobs = new Map();

const createJobRecord = ({ jobId, userId, playerNick, fileName, fileSize }) => {
  const now = new Date().toISOString();
  const record = {
    jobId,
    userId,
    playerNick,
    fileName,
    fileSize,
    status: 'queued',
    stage: 'queued',
    progressMessage: 'ÐÐ°Ð´Ð°Ð½Ð¸Ðµ Ð¿Ð¾ÑÑÐ°Ð²Ð»ÐµÐ½Ð¾ Ð² Ð¾ÑÐµÑÐµÐ´Ñ Ð½Ð° Ð¾Ð±ÑÐ°Ð±Ð¾ÑÐºÑ.',
    createdAt: now,
    updatedAt: now,
    decompressedZst: fileName.toLowerCase().endsWith('.dem.zst'),
    replayPrepared: false,
    kills: null,
    error: null
  };
  jobs.set(jobId, record);
  return record;
};

const updateJob = (jobId, patch) => {
  const current = jobs.get(jobId);
  if (!current) return null;
  const next = {
    ...current,
    ...patch,
    updatedAt: new Date().toISOString()
  };
  jobs.set(jobId, next);
  return next;
};

setInterval(() => {
  const cutoff = Date.now() - JOB_TTL_MS;
  for (const [id, job] of jobs.entries()) {
    const createdTime = Date.parse(job.createdAt || '');
    if (Number.isFinite(createdTime) && createdTime < cutoff) {
      jobs.delete(id);
    }
  }
}, 5 * 60 * 1000).unref();

const storage = multer.diskStorage({
  destination(_req, _file, cb) {
    cb(null, tempUploadDir);
  },
  filename(req, file, cb) {
    const safeBase = path.basename(file.originalname || 'upload.dem').replace(/[^a-zA-Z0-9._-]/g, '_');
    const uniquePrefix = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const storedName = `${uniquePrefix}_${safeBase}`;
    logInfo('multer-file-init', `Receiving file "${file.originalname}" -> "${storedName}"`, {
      reqId: req.reqId,
      originalname: file.originalname,
      mimetype: file.mimetype,
      storedName
    });
    cb(null, storedName);
  }
});

const isSupportedDemoName = filename => {
  const lower = String(filename || '').trim().toLowerCase();
  return lower.endsWith('.dem.zst') || lower.endsWith('.dem');
};

const upload = multer({
  storage,
  limits: {
    fileSize: MAX_UPLOAD_BYTES,
    files: 1
  },
  fileFilter(req, file, cb) {
    if (!isSupportedDemoName(file.originalname)) {
      const err = new Error('ÐÐ¾Ð¿ÑÑÐºÐ°ÑÑÑÑ ÑÐ¾Ð»ÑÐºÐ¾ ÑÐ°Ð¹Ð»Ñ .dem Ð¸ .dem.zst.');
      err.statusCode = 400;
      logError('multer-file-filter', err, {
        reqId: req.reqId,
        originalname: file.originalname
      });
      return cb(err);
    }
    cb(null, true);
  }
});

const hasMagicBytes = async (filePath, magicBytes) => {
  const handle = await fs.promises.open(filePath, 'r');
  try {
    const buf = Buffer.alloc(magicBytes.length);
    const { bytesRead } = await handle.read(buf, 0, magicBytes.length, 0);
    if (bytesRead < magicBytes.length) return false;
    for (let i = 0; i < magicBytes.length; i++) {
      if (buf[i] !== magicBytes[i]) return false;
    }
    return true;
  } finally {
    await handle.close();
  }
};

const ZSTD_MAGIC = [0x28, 0xb5, 0x2f, 0xfd];
const CS2_DEMO_MAGIC = [0x50, 0x42, 0x44, 0x45, 0x4d, 0x53, 0x32]; // "PBDEMS2"

const hasZstdCli = async () => {
  try {
    await execFileAsync('zstd', ['--version'], { timeout: 5000 });
    return true;
  } catch {
    return false;
  }
};

const yieldToEventLoop = () => new Promise(resolve => setImmediate(resolve));

// ÐÐ¾ÑÐ¾ÐºÐ¾Ð²Ð°Ñ ÑÐ°ÑÐ¿Ð°ÐºÐ¾Ð²ÐºÐ° .dem.zst ÑÐµÑÐµÐ· fzstd.Decompress Ñ ÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¼ backpressure
// Ð¸ ÑÑÑÑÐ¿ÐºÐ¾Ð¹ event loop Ð¼ÐµÐ¶Ð´Ñ ÑÐ°Ð½ÐºÐ°Ð¼Ð¸, ÑÑÐ¾Ð±Ñ Ð½Ðµ ÑÐ´ÐµÑÐ¶Ð¸Ð²Ð°ÑÑ HTTP-ÑÐ¾ÐºÐµÑÑ Ð¸ Ð½Ðµ Ð±Ð»Ð¾ÐºÐ¸ÑÐ¾Ð²Ð°ÑÑ event loop
const decompressZstStream = async (zstPath, outDemPath, reqId) => {
  let unpackedBytes = 0;
  let lastLoggedMB = 0;
  let chunksSinceYield = 0;
  let streamError = null;

  const readStream = fs.createReadStream(zstPath, { highWaterMark: 256 * 1024 });
  const writeStream = fs.createWriteStream(outDemPath, { highWaterMark: 256 * 1024 });

  const abortStreams = err => {
    if (!streamError) {
      streamError = err;
    }
    readStream.destroy(err);
    writeStream.destroy(err);
  };

  const writeChunkWithBackpressure = chunk =>
    new Promise((resolve, reject) => {
      if (streamError) {
        reject(streamError);
        return;
      }
      if (writeStream.closed || writeStream.destroyed) {
        reject(new Error('ÐÐ¾ÑÐ¾Ðº Ð·Ð°Ð¿Ð¸ÑÐ¸ ÑÐ°ÑÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð½Ð¾Ð³Ð¾ ÑÐ°Ð¹Ð»Ð° Ð±ÑÐ» Ð·Ð°ÐºÑÑÑ Ð´Ð¾ Ð·Ð°Ð²ÐµÑÑÐµÐ½Ð¸Ñ.'));
        return;
      }
      const canContinue = writeStream.write(chunk);
      if (canContinue) {
        resolve();
      } else {
        const onDrain = () => {
          cleanupListeners();
          resolve();
        };
        const onClose = () => {
          cleanupListeners();
          reject(new Error('ÐÐ¾ÑÐ¾Ðº Ð·Ð°Ð¿Ð¸ÑÐ¸ Ð·Ð°ÐºÑÑÑ Ð²Ð¾ Ð²ÑÐµÐ¼Ñ Ð¾Ð¶Ð¸Ð´Ð°Ð½Ð¸Ñ drain.'));
        };
        const onError = err => {
          cleanupListeners();
          reject(err);
        };
        const cleanupListeners = () => {
          writeStream.off('drain', onDrain);
          writeStream.off('close', onClose);
          writeStream.off('error', onError);
        };
        writeStream.once('drain', onDrain);
        writeStream.once('close', onClose);
        writeStream.once('error', onError);
      }
    });

  const endWriteStream = () =>
    new Promise((resolve, reject) => {
      if (writeStream.writableFinished) {
        resolve();
        return;
      }
      if (writeStream.closed || writeStream.destroyed) {
        resolve();
        return;
      }
      const onFinish = () => {
        cleanupListeners();
        resolve();
      };
      const onError = err => {
        cleanupListeners();
        reject(err);
      };
      const cleanupListeners = () => {
        writeStream.off('finish', onFinish);
        writeStream.off('error', onError);
      };
      writeStream.once('finish', onFinish);
      writeStream.once('error', onError);
      writeStream.end();
    });

  const decoder = new FzstdStreamDecompress((chunk, _final) => {
    if (streamError) return;
    if (chunk && chunk.length > 0) {
      unpackedBytes += chunk.length;
      if (unpackedBytes > MAX_UNPACKED_BYTES) {
        abortStreams(
          new Error(`Ð Ð°ÑÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð½ÑÐ¹ ÑÐ°Ð¹Ð» Ð¿ÑÐµÐ²ÑÑÐ¸Ð» Ð»Ð¸Ð¼Ð¸Ñ ${Math.round(MAX_UNPACKED_BYTES / (1024 * 1024))} ÐÐ.`)
        );
        return;
      }
      const currentMB = Math.floor(unpackedBytes / (100 * 1024 * 1024));
      if (currentMB > lastLoggedMB) {
        lastLoggedMB = currentMB;
        logInfo('zstd-stream-progress', `Decompressed ${Math.round(unpackedBytes / (1024 * 1024))} MB so far`, {
          reqId,
          unpackedBytes
        });
      }
      writeStream.write(Buffer.from(chunk));
    }
  });

  try {
    for await (const rawChunk of readStream) {
      if (streamError) {
        throw streamError;
      }
      const bytes = new Uint8Array(rawChunk.buffer, rawChunk.byteOffset, rawChunk.byteLength);
      decoder.push(bytes, false);

      if (streamError) {
        throw streamError;
      }

      if (writeStream.writableNeedDrain) {
        await writeChunkWithBackpressure(Buffer.alloc(0));
      }

      chunksSinceYield += 1;
      if (chunksSinceYield >= 8) {
        chunksSinceYield = 0;
        await yieldToEventLoop();
      }
    }

    decoder.push(new Uint8Array(0), true);
    if (streamError) {
      throw streamError;
    }

    await endWriteStream();

    if (unpackedBytes < 16) {
      throw new Error('Ð Ð°ÑÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð½ÑÐ¹ Ð¿Ð¾ÑÐ¾Ðº Zstandard Ð¿ÑÑÑ Ð¸Ð»Ð¸ Ð¿Ð¾Ð²ÑÐµÐ¶Ð´ÑÐ½.');
    }
  } catch (err) {
    abortStreams(err);
    throw err;
  } finally {
    if (!readStream.closed && !readStream.destroyed) {
      readStream.destroy();
    }
    if (!writeStream.closed && !writeStream.destroyed) {
      writeStream.destroy();
    }
  }
};

const decompressZstFile = async (zstPath, outDemPath, reqId) => {
  const cliAvailable = await hasZstdCli();
  if (cliAvailable) {
    logInfo('zstd-unpack', `Decompressing via zstd CLI: ${zstPath} -> ${outDemPath}`, { reqId });
    await execFileAsync('zstd', ['-d', '-f', '--no-progress', zstPath, '-o', outDemPath], {
      maxBuffer: 10 * 1024 * 1024
    });
    return;
  }

  logInfo('zstd-unpack', `Decompressing via streaming fzstd.Decompress: ${zstPath} -> ${outDemPath}`, { reqId });
  await decompressZstStream(zstPath, outDemPath, reqId);
};

const normalizeNick = value => String(value || '').trim().toLowerCase();

const parseKillsFromDemFile = async (demPath, playerNick, reqId) => {
  const stat = await fs.promises.stat(demPath);
  if (!stat.isFile() || stat.size < 16) {
    throw new Error('Ð Ð°ÑÐ¿Ð°ÐºÐ¾Ð²Ð°Ð½Ð½ÑÐ¹ ÑÐ°Ð¹Ð» .dem Ð¿ÑÑÑ Ð¸Ð»Ð¸ Ð¿Ð¾Ð²ÑÐµÐ¶Ð´ÑÐ½.');
  }

  const isSource2Demo = await hasMagicBytes(demPath, CS2_DEMO_MAGIC);
  if (!isSource2Demo) {
    throw new Error('Ð¤Ð°Ð¹Ð» Ð½Ðµ ÑÐ¾Ð´ÐµÑÐ¶Ð¸Ñ ÐºÐ¾ÑÑÐµÐºÑÐ½Ð¾Ð³Ð¾ Ð·Ð°Ð³Ð¾Ð»Ð¾Ð²ÐºÐ° CS2 Source 2 (PBDEMS2).');
  }

  logInfo('demoparser2', `Parsing demo header and player_death events for "${playerNick}"`, {
    reqId,
    demPath,
    demSizeBytes: stat.size,
    playerNick
  });

  parseHeader(demPath);

  const deathEvents = parseEvent(demPath, 'player_death', ['total_rounds_played', 'game_time', 'round_time']);
  if (!Array.isArray(deathEvents)) {
    throw new Error('ÐÐ°ÑÑÐµÑ @laihoe/demoparser2 Ð²ÐµÑÐ½ÑÐ» Ð½ÐµÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ ÑÐ¾ÑÐ¼Ð°Ñ ÑÐ¾Ð±ÑÑÐ¸Ð¹ player_death.');
  }

  const targetNick = normalizeNick(playerNick);
  const matchedKills = [];

  for (const ev of deathEvents) {
    if (!ev || typeof ev !== 'object') continue;

    const attackerName =
      ev.attacker_name ??
      ev.attackerName ??
      ev.user_name ??
      ev.attacker_player_name ??
      '';

    if (normalizeNick(attackerName) !== targetNick) {
      continue;
    }

    const victimName =
      ev.user_name ??
      ev.victim_name ??
      ev.victimName ??
      ev.player_name ??
      '';

    const weaponName =
      ev.weapon ??
      ev.weapon_name ??
      ev.weaponName ??
      '';

    const tickNumber = Number.isFinite(Number(ev.tick)) ? Number(ev.tick) : null;
    const headshotBool = Boolean(ev.headshot);

    const rawRound = ev.round ?? ev.total_rounds_played ?? ev.round_number;
    const roundNumber = Number.isFinite(Number(rawRound)) ? Number(rawRound) : null;

    matchedKills.push({
      tick: tickNumber,
      weapon: String(weaponName || ''),
      victim: String(victimName || ''),
      headshot: headshotBool,
      round: roundNumber,
      attacker: String(attackerName),
      replayReady: tickNumber !== null
    });
  }

  return matchedKills;
};

const runDemoJobInBackground = async ({ jobId, userId, uploadedPath, originalName, fileSize, playerNick }) => {
  const filesToCleanup = [uploadedPath];
  const isZst = originalName.toLowerCase().endsWith('.dem.zst');
  let workingDemPath = uploadedPath;
  const startedAt = Date.now();

  try {
    if (isZst) {
      updateJob(jobId, {
        status: 'decompressing',
        stage: 'decompressing',
        progressMessage: 'Ð Ð°ÑÐ¿Ð°ÐºÐ¾Ð²ÐºÐ° Ð°ÑÑÐ¸Ð²Ð° .dem.zst...'
      });

      logInfo('decompress-start', `Starting .dem.zst decompression for "${originalName}" (${fileSize} bytes)`, {
        reqId: jobId,
        userId,
        fileName: originalName,
        fileSize
      });

      const validZst = await hasMagicBytes(uploadedPath, ZSTD_MAGIC);
      if (!validZst) {
        throw new Error('Ð¤Ð°Ð¹Ð» Ð¸Ð¼ÐµÐµÑ ÑÐ°ÑÑÐ¸ÑÐµÐ½Ð¸Ðµ .dem.zst, Ð½Ð¾ Ð½Ðµ ÑÐ¾Ð´ÐµÑÐ¶Ð¸Ñ Ð²Ð°Ð»Ð¸Ð´Ð½ÑÐ¹ Ð·Ð°Ð³Ð¾Ð»Ð¾Ð²Ð¾Ðº Zstandard.');
      }

      const unpackedDemPath = path.join(
        tempUploadDir,
        `${path.basename(uploadedPath, '.zst')}_${Date.now()}.dem`
      );
      filesToCleanup.push(unpackedDemPath);

      const decompressStart = Date.now();
      await decompressZstFile(uploadedPath, unpackedDemPath, jobId);
      workingDemPath = unpackedDemPath;

      const unpackedStat = await fs.promises.stat(unpackedDemPath);
      logInfo(
        'decompress-done',
        `Finished .dem.zst decompression -> "${path.basename(unpackedDemPath)}" (${unpackedStat.size} bytes) in ${Date.now() - decompressStart} ms`,
        {
          reqId: jobId,
          userId,
          sourceFile: originalName,
          unpackedFile: path.basename(unpackedDemPath),
          unpackedSizeBytes: unpackedStat.size,
          durationMs: Date.now() - decompressStart
        }
      );
    }

    updateJob(jobId, {
      status: 'parsing',
      stage: 'parsing',
      progressMessage: 'ÐÐ½Ð°Ð»Ð¸Ð· ÑÐ¾Ð±ÑÑÐ¸Ð¹ Ð´ÐµÐ¼Ð¾Ð·Ð°Ð¿Ð¸ÑÐ¸ ÑÐµÑÐµÐ· @laihoe/demoparser2...'
    });

    logInfo('parse-start', `Starting @laihoe/demoparser2 analysis for playerNick="${playerNick}"`, {
      reqId: jobId,
      userId,
      fileName: originalName,
      playerNick
    });

    const parseStart = Date.now();
    const kills = await parseKillsFromDemFile(workingDemPath, playerNick, jobId);
    const parseDurationMs = Date.now() - parseStart;
    const elapsedMs = Date.now() - startedAt;

    logInfo(
      'parse-done',
      `Completed @laihoe/demoparser2 analysis: found ${kills.length} kills for "${playerNick}" in ${parseDurationMs} ms (total ${elapsedMs} ms)`,
      {
        reqId: jobId,
        userId,
        nick: playerNick,
        file: originalName,
        killsCount: kills.length,
        parseDurationMs,
        totalElapsedMs: elapsedMs
      }
    );

    updateJob(jobId, {
      status: 'completed',
      stage: 'completed',
      progressMessage: 'ÐÐ±ÑÐ°Ð±Ð¾ÑÐºÐ° Ð´ÐµÐ¼Ð¾Ð·Ð°Ð¿Ð¸ÑÐ¸ Ð·Ð°Ð²ÐµÑÑÐµÐ½Ð°.',
      decompressedZst: isZst,
      replayPrepared: kills.length > 0,
      kills,
      error: null
    });
  } catch (err) {
    logError('job-failed', err, {
      reqId: jobId,
      userId,
      fileName: originalName,
      fileSize,
      playerNick,
      elapsedMs: Date.now() - startedAt
    });

    updateJob(jobId, {
      status: 'failed',
      stage: 'failed',
      progressMessage: 'ÐÑÐ¸Ð±ÐºÐ° Ð¿ÑÐ¸ Ð¾Ð±ÑÐ°Ð±Ð¾ÑÐºÐµ ÑÐ°Ð¹Ð»Ð° Ð´ÐµÐ¼Ð¾Ð·Ð°Ð¿Ð¸ÑÐ¸.',
      error: err?.message || 'ÐÑÐ¸Ð±ÐºÐ° Ð¿ÑÐ¸ Ð¾Ð±ÑÐ°Ð±Ð¾ÑÐºÐµ ÑÐ°Ð¹Ð»Ð° Ð´ÐµÐ¼Ð¾Ð·Ð°Ð¿Ð¸ÑÐ¸ Ð½Ð° ÑÐµÑÐ²ÐµÑÐµ.'
    });
  } finally {
    await cleanupFiles(filesToCleanup, jobId);
  }
};

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'cs2-kill-editor-server', time: ts() });
});

// ÐÐ°Ð²ÐµÑÑÐ°ÐµÐ¼ HTTP-Ð¾ÑÐ²ÐµÑ Ð´Ð¾ Ð·Ð°Ð¿ÑÑÐºÐ° ÑÑÐ¶ÑÐ»Ð¾Ð¹ ÑÐ¾Ð½Ð¾Ð²Ð¾Ð¹ Ð·Ð°Ð´Ð°ÑÐ¸, ÑÑÐ¾Ð±Ñ ÑÐ¾ÐºÐµÑ Ð¸ Ð·Ð°Ð¿ÑÐ¾Ñ Ð¾ÑÐ²Ð¾Ð±Ð¾Ð¶Ð´Ð°Ð»Ð¸ÑÑ ÑÑÐ°Ð·Ñ
const handleStartAnalyzeJob = async (req, res) => {
  const uploadedFile = req.file;
  const playerNick = typeof req.body?.playerNick === 'string' ? req.body.playerNick.trim() : '';

  if (!uploadedFile) {
    logError('validation', new Error('Missing multipart file "demo"'), { reqId: req.reqId });
    return res.status(400).json({
      message: 'ÐÐµ Ð¿Ð¾Ð»ÑÑÐµÐ½ ÑÐ°Ð¹Ð» Ð² Ð¿Ð¾Ð»Ðµ multipart "demo".'
    });
  }

  if (!playerNick) {
    await cleanupFiles([uploadedFile.path], req.reqId);
    logError('validation', new Error('Missing required field "playerNick"'), { reqId: req.reqId });
    return res.status(400).json({
      message: 'ÐÐµ ÑÐºÐ°Ð·Ð°Ð½ Ð¾Ð±ÑÐ·Ð°ÑÐµÐ»ÑÐ½ÑÐ¹ Ð¿Ð°ÑÐ°Ð¼ÐµÑÑ "playerNick".'
    });
  }

  const jobId = crypto.randomUUID();
  const originalName = uploadedFile.originalname || 'match.dem';

  createJobRecord({
    jobId,
    userId: req.supabaseUser.id,
    playerNick,
    fileName: originalName,
    fileSize: uploadedFile.size
  });

  logInfo('job-created', `Created background job ${jobId} for "${originalName}" (${uploadedFile.size} bytes)`, {
    reqId: req.reqId,
    jobId,
    userId: req.supabaseUser.id,
    playerNick,
    fileName: originalName,
    fileSize: uploadedFile.size
  });

  res.status(202).json({
    ok: true,
    jobId,
    status: 'queued',
    playerNick,
    fileName: originalName,
    statusUrl: `/api/v1/demos/status/${jobId}`
  });

  res.once('finish', () => {
    setImmediate(() => {
      runDemoJobInBackground({
        jobId,
        userId: req.supabaseUser.id,
        uploadedPath: uploadedFile.path,
        originalName,
        fileSize: uploadedFile.size,
        playerNick
      });
    });
  });
};

app.post(
  '/api/v1/demos/analyze',
  requireSupabaseAuth,
  (req, res, next) => {
    logInfo('multer-upload-start', `Starting Multer upload for ${req.originalUrl}`, {
      reqId: req.reqId,
      userId: req.supabaseUser?.id,
      contentLength: req.headers['content-length'] || 'unknown'
    });

    const uploadStart = Date.now();

    upload.single('demo')(req, res, err => {
      if (err) {
        logError('multer-upload-error', err, {
          reqId: req.reqId,
          userId: req.supabaseUser?.id,
          contentLength: req.headers['content-length'] || 'unknown',
          durationMs: Date.now() - uploadStart
        });

        if (err instanceof multer.MulterError) {
          if (err.code === 'LIMIT_FILE_SIZE') {
            return res.status(413).json({
              message: `Ð Ð°Ð·Ð¼ÐµÑ ÑÐ°Ð¹Ð»Ð° Ð¿ÑÐµÐ²ÑÑÐ°ÐµÑ Ð´Ð¾Ð¿ÑÑÑÐ¸Ð¼ÑÐ¹ Ð»Ð¸Ð¼Ð¸Ñ (${Math.round(MAX_UPLOAD_BYTES / (1024 * 1024))} ÐÐ).`
            });
          }
          return res.status(400).json({
            message: `ÐÑÐ¸Ð±ÐºÐ° Ð·Ð°Ð³ÑÑÐ·ÐºÐ¸ ÑÐ°Ð¹Ð»Ð° (${err.code}): ${err.message}`
          });
        }

        return res.status(err.statusCode || 400).json({
          message: err.message || 'ÐÐµÐºÐ¾ÑÑÐµÐºÑÐ½ÑÐ¹ multipart/form-data Ð·Ð°Ð¿ÑÐ¾Ñ.'
        });
      }

      logInfo(
        'multer-upload-done',
        `Multer upload completed: file="${req.file?.originalname || 'none'}", size=${req.file?.size ?? 0} bytes in ${Date.now() - uploadStart} ms`,
        {
          reqId: req.reqId,
          userId: req.supabaseUser?.id,
          fileName: req.file?.originalname || null,
          storedPath: req.file?.path || null,
          fileSizeBytes: req.file?.size ?? 0,
          playerNick: req.body?.playerNick || null,
          durationMs: Date.now() - uploadStart
        }
      );

      return next();
    });
  },
  handleStartAnalyzeJob
);

app.get('/api/v1/demos/status/:jobId', requireSupabaseAuth, (req, res) => {
  const { jobId } = req.params;
  const job = jobs.get(jobId);

  if (!job) {
    logInfo('job-status-not-found', `Job ${jobId} not found (may have expired or server restarted)`, {
      reqId: req.reqId,
      jobId,
      userId: req.supabaseUser.id
    });
    return res.status(404).json({
      ok: false,
      jobId,
      status: 'not_found',
      message: 'ÐÐ°Ð´Ð°Ð½Ð¸Ðµ Ð½Ðµ Ð½Ð°Ð¹Ð´ÐµÐ½Ð¾. ÐÐ¾Ð·Ð¼Ð¾Ð¶Ð½Ð¾, Ð¾Ð½Ð¾ Ð¸ÑÑÐµÐºÐ»Ð¾ Ð¸Ð»Ð¸ ÑÐµÑÐ²ÐµÑ Render Ð¿ÐµÑÐµÐ·Ð°Ð¿ÑÑÐºÐ°Ð»ÑÑ.'
    });
  }

  if (job.userId !== req.supabaseUser.id) {
    logError('job-status-forbidden', new Error('User attempted to access another user job'), {
      reqId: req.reqId,
      jobId,
      requestUserId: req.supabaseUser.id,
      ownerUserId: job.userId
    });
    return res.status(403).json({
      ok: false,
      message: 'ÐÐ¾ÑÑÑÐ¿ Ðº ÑÑÐ¾Ð¼Ñ Ð·Ð°Ð´Ð°Ð½Ð¸Ñ Ð·Ð°Ð¿ÑÐµÑÑÐ½.'
    });
  }

  if (job.status === 'completed') {
    return res.status(200).json({
      ok: true,
      jobId: job.jobId,
      status: job.status,
      stage: job.stage,
      progressMessage: job.progressMessage,
      playerNick: job.playerNick,
      fileName: job.fileName,
      decompressedZst: job.decompressedZst,
      replayPrepared: job.replayPrepared,
      kills: job.kills,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt
    });
  }

  if (job.status === 'failed') {
    return res.status(200).json({
      ok: false,
      jobId: job.jobId,
      status: job.status,
      stage: job.stage,
      progressMessage: job.progressMessage,
      playerNick: job.playerNick,
      fileName: job.fileName,
      error: job.error,
      message: job.error,
      createdAt: job.createdAt,
      updatedAt: job.updatedAt
    });
  }

  return res.status(200).json({
    ok: true,
    jobId: job.jobId,
    status: job.status,
    stage: job.stage,
    progressMessage: job.progressMessage,
    playerNick: job.playerNick,
    fileName: job.fileName,
    createdAt: job.createdAt,
    updatedAt: job.updatedAt
  });
});

app.use((err, req, res, _next) => {
  logError('unhandled-express', err, { reqId: req?.reqId });
  const status = Number.isInteger(err?.statusCode) ? err.statusCode : 500;
  res.status(status).json({
    message: err?.message || 'ÐÐ½ÑÑÑÐµÐ½Ð½ÑÑ Ð¾ÑÐ¸Ð±ÐºÐ° ÑÐµÑÐ²ÐµÑÐ°.'
  });
});

process.on('unhandledRejection', reason => {
  logError('unhandledRejection', reason instanceof Error ? reason : new Error(String(reason)));
});

process.on('uncaughtException', err => {
  logError('uncaughtException', err);
});

const server = app.listen(PORT, '0.0.0.0', () => {
  logInfo('server-start', `Listening on 0.0.0.0:${PORT} (node=${process.version})`, {
    port: PORT,
    nodeVersion: process.version,
    tempDir: tempUploadDir,
    maxUploadMB: Math.round(MAX_UPLOAD_BYTES / (1024 * 1024)),
    maxUnpackedMB: Math.round(MAX_UNPACKED_BYTES / (1024 * 1024)),
    requestTimeoutMs: REQUEST_TIMEOUT_MS
  });
});

server.requestTimeout = REQUEST_TIMEOUT_MS;
server.headersTimeout = REQUEST_TIMEOUT_MS + 10000;
server.keepAliveTimeout = 65000;

server.on('timeout', socket => {
  logError('server-timeout', new Error('HTTP server request/socket timed out'), {
    remoteAddress: socket?.remoteAddress,
    remotePort: socket?.remotePort
  });
});
