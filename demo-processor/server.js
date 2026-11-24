import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { createClient } from '@supabase/supabase-js';
import { parseEvent, parseHeader, parsePlayerInfo } from '@laihoe/demoparser2';
import { Decompress as FzstdStreamDecompress } from 'fzstd';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
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
const MAX_UNPACKED_BYTES = Number(process.env.MAX_UNPACKED_BYTES) || 1200 * 1024 * 1024; // 1.2 GB на диске
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS) || 3 * 60 * 1000; // 3 мин на upload
const JOB_TTL_MS = Number(process.env.JOB_TTL_MS) || 60 * 60 * 1000; // 1 час хранения статуса в памяти

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
        message: 'Требуется авторизация: отсутствует Bearer access_token в заголовке Authorization.'
      });
    }

    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user) {
      logError('auth', error || new Error('Invalid or expired token'), {
        reqId: req.reqId,
        path: req.originalUrl
      });
      return res.status(401).json({
        message: 'Недействительный или истёкший токен авторизации Supabase.'
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
      message: 'Внутренняя ошибка при проверке авторизации Supabase.'
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
    progressMessage: 'Задание поставлено в очередь на обработку.',
    createdAt: now,
    updatedAt: now,
    decompressedZst: fileName.toLowerCase().endsWith('.dem.zst'),
    replayPrepared: false,
    kills: null,
    detectedPlayers: [],
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
      const err = new Error('Допускаются только файлы .dem и .dem.zst.');
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
        reject(new Error('Поток записи распакованного файла был закрыт до завершения.'));
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
          reject(new Error('Поток записи закрыт во время ожидания drain.'));
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
          new Error(`Распакованный файл превысил лимит ${Math.round(MAX_UNPACKED_BYTES / (1024 * 1024))} МБ.`)
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
      throw new Error('Распакованный поток Zstandard пуст или повреждён.');
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

const normalizeStr = value => String(value ?? '').trim().toLowerCase();

const normalizeSteamId = value => {
  if (value === null || value === undefined) return '';
  const str = String(value).trim();
  if (!str || str === '0') return '';
  return str;
};

// Извлекает SteamID (64-битный строковый идентификатор) и имя игрока из полей события или списка игроков демо
const extractActorIdentity = (ev, prefix, playerInfoBySteamId, playerInfoByName) => {
  const rawSteamId =
    ev?.[`${prefix}_steamid`] ??
    ev?.[`${prefix}_steam_id`] ??
    ev?.[`${prefix}SteamId`] ??
    null;

  const steamId = normalizeSteamId(rawSteamId);

  const rawName =
    ev?.[`${prefix}_name`] ??
    ev?.[`${prefix}Name`] ??
    '';

  let name = String(rawName ?? '').trim();

  if (steamId && playerInfoBySteamId.has(steamId)) {
    const info = playerInfoBySteamId.get(steamId);
    if (!name && info.name) {
      name = info.name;
    }
  } else if (!steamId && name) {
    const matchedByName = playerInfoByName.get(normalizeStr(name));
    if (matchedByName?.steamId) {
      return {
        steamId: matchedByName.steamId,
        name: name || matchedByName.name
      };
    }
  }

  return { steamId, name };
};

const parseKillsFromDemFile = async (demPath, playerNick, reqId) => {
  const stat = await fs.promises.stat(demPath);
  if (!stat.isFile() || stat.size < 16) {
    throw new Error('Распакованный файл .dem пуст или повреждён.');
  }

  const isSource2Demo = await hasMagicBytes(demPath, CS2_DEMO_MAGIC);
  if (!isSource2Demo) {
    throw new Error('Файл не содержит корректного заголовка CS2 Source 2 (PBDEMS2).');
  }

  if (typeof parseHeader !== 'function' || typeof parseEvent !== 'function') {
    throw new Error('Несовместимый API @laihoe/demoparser2: отсутствуют parseHeader или parseEvent.');
  }

  logInfo('demoparser2', `Parsing demo header, player info and player_death events`, {
    reqId,
    demPath,
    demSizeBytes: stat.size,
    query: playerNick
  });

  parseHeader(demPath);

  // Собираем справочник игроков через parsePlayerInfo (если поддерживается текущей версией пакета)
  const playerInfoBySteamId = new Map();
  const playerInfoByName = new Map();
  const playersMap = new Map();

  const registerPlayer = (steamId, name) => {
    const cleanSteamId = normalizeSteamId(steamId);
    const cleanName = String(name ?? '').trim();
    if (!cleanSteamId && !cleanName) return;

    if (cleanSteamId) {
      const existing = playerInfoBySteamId.get(cleanSteamId) || { steamId: cleanSteamId, name: '' };
      if (cleanName) existing.name = cleanName;
      playerInfoBySteamId.set(cleanSteamId, existing);
    }

    if (cleanName) {
      const normKey = normalizeStr(cleanName);
      if (!playerInfoByName.has(normKey)) {
        playerInfoByName.set(normKey, { steamId: cleanSteamId, name: cleanName });
      }
    }

    const mapKey = cleanSteamId || `name:${normalizeStr(cleanName)}`;
    if (!playersMap.has(mapKey)) {
      playersMap.set(mapKey, {
        name: cleanName || cleanSteamId,
        steamId: cleanSteamId || null
      });
    } else if (cleanName && !playersMap.get(mapKey).name) {
      playersMap.get(mapKey).name = cleanName;
    }
  };

  if (typeof parsePlayerInfo === 'function') {
    try {
      const infoList = parsePlayerInfo(demPath);
      if (Array.isArray(infoList)) {
        for (const p of infoList) {
          if (!p || typeof p !== 'object') continue;
          const pSteamId = p.steamid ?? p.steam_id ?? p.steamId ?? null;
          const pName = p.name ?? p.player_name ?? p.user_name ?? '';
          registerPlayer(pSteamId, pName);
        }
      }
    } catch (infoErr) {
      logError('demoparser2-player-info', infoErr, { reqId });
    }
  }

  const deathEvents = parseEvent(demPath, 'player_death', ['total_rounds_played', 'game_time', 'round_time']);
  if (!Array.isArray(deathEvents)) {
    throw new Error('Парсер @laihoe/demoparser2 вернул некорректный формат событий player_death.');
  }

  const totalPlayerDeathEvents = deathEvents.length;
  let eventsWithAttacker = 0;

  for (const ev of deathEvents) {
    if (!ev || typeof ev !== 'object') continue;
    const attacker = extractActorIdentity(ev, 'attacker', playerInfoBySteamId, playerInfoByName);
    const victim = extractActorIdentity(ev, 'user', playerInfoBySteamId, playerInfoByName);
    if (!victim.steamId && !victim.name) {
      const fallbackVictim = extractActorIdentity(ev, 'victim', playerInfoBySteamId, playerInfoByName);
      registerPlayer(fallbackVictim.steamId, fallbackVictim.name);
    } else {
      registerPlayer(victim.steamId, victim.name);
    }

    if (attacker.steamId || attacker.name) {
      eventsWithAttacker += 1;
      registerPlayer(attacker.steamId, attacker.name);
    }
  }

  const queryRaw = String(playerNick || '').trim();
  const queryLower = normalizeStr(queryRaw);
  const querySteamId = normalizeSteamId(queryRaw);

  const isMatchedActor = actor => {
    if (!actor) return false;
    if (querySteamId && actor.steamId && actor.steamId === querySteamId) {
      return true;
    }
    if (queryLower && actor.name && normalizeStr(actor.name) === queryLower) {
      return true;
    }
    return false;
  };

  const matchedKills = [];

  for (const ev of deathEvents) {
    if (!ev || typeof ev !== 'object') continue;

    const attacker = extractActorIdentity(ev, 'attacker', playerInfoBySteamId, playerInfoByName);
    if (!isMatchedActor(attacker)) {
      continue;
    }

    let victim = extractActorIdentity(ev, 'user', playerInfoBySteamId, playerInfoByName);
    if (!victim.steamId && !victim.name) {
      victim = extractActorIdentity(ev, 'victim', playerInfoBySteamId, playerInfoByName);
    }

    const weaponName =
      ev.weapon ??
      ev.weapon_name ??
      '';

    const tickNumber = Number.isFinite(Number(ev.tick)) ? Number(ev.tick) : null;
    const headshotBool = Boolean(ev.headshot);

    const rawRound = ev.round ?? ev.total_rounds_played;
    const roundNumber = Number.isFinite(Number(rawRound)) ? Number(rawRound) : null;

    matchedKills.push({
      tick: tickNumber,
      weapon: String(weaponName || ''),
      victim: String(victim.name || ''),
      victimSteamId: victim.steamId || null,
      headshot: headshotBool,
      round: roundNumber,
      attacker: String(attacker.name || ''),
      attackerSteamId: attacker.steamId || null,
      replayReady: tickNumber !== null
    });
  }

  const detectedPlayers = Array.from(playersMap.values()).sort((a, b) =>
    a.name.localeCompare(b.name, undefined, { sensitivity: 'base' })
  );

  logInfo('demoparser2-diagnostics', 'player_death diagnostics summary', {
    reqId,
    query: queryRaw,
    totalPlayerDeathEvents,
    eventsWithAttacker,
    matchedKillsCount: matchedKills.length,
    detectedPlayersCount: detectedPlayers.length
  });

  return {
    kills: matchedKills,
    detectedPlayers,
    diagnostics: {
      totalPlayerDeathEvents,
      eventsWithAttacker,
      matchedKillsCount: matchedKills.length,
      detectedPlayersCount: detectedPlayers.length
    }
  };
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
        progressMessage: 'Распаковка архива .dem.zst...'
      });

      logInfo('decompress-start', `Starting .dem.zst decompression for "${originalName}" (${fileSize} bytes)`, {
        reqId: jobId,
        userId,
        fileName: originalName,
        fileSize
      });

      const validZst = await hasMagicBytes(uploadedPath, ZSTD_MAGIC);
      if (!validZst) {
        throw new Error('Файл имеет расширение .dem.zst, но не содержит валидный заголовок Zstandard.');
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
      progressMessage: 'Анализ событий демозаписи через @laihoe/demoparser2...'
    });

    logInfo('parse-start', `Starting @laihoe/demoparser2 analysis for query="${playerNick}"`, {
      reqId: jobId,
      userId,
      fileName: originalName,
      playerNick
    });

    const parseStart = Date.now();
    const { kills, detectedPlayers, diagnostics } = await parseKillsFromDemFile(
      workingDemPath,
      playerNick,
      jobId
    );
    const parseDurationMs = Date.now() - parseStart;
    const elapsedMs = Date.now() - startedAt;

    logInfo(
      'parse-done',
      `Completed @laihoe/demoparser2 analysis: total_player_death=${diagnostics.totalPlayerDeathEvents}, with_attacker=${diagnostics.eventsWithAttacker}, player_kills=${kills.length} in ${parseDurationMs} ms (total ${elapsedMs} ms)`,
      {
        reqId: jobId,
        userId,
        nick: playerNick,
        file: originalName,
        totalPlayerDeathEvents: diagnostics.totalPlayerDeathEvents,
        eventsWithAttacker: diagnostics.eventsWithAttacker,
        killsCount: kills.length,
        detectedPlayersCount: detectedPlayers.length,
        parseDurationMs,
        totalElapsedMs: elapsedMs
      }
    );

    const progressMessage =
      kills.length > 0
        ? `Обработка завершена: найдено убийств — ${kills.length}.`
        : `Обработка завершена: убийства для "${playerNick}" не найдены (всего player_death в демо: ${diagnostics.totalPlayerDeathEvents}).`;

    updateJob(jobId, {
      status: 'completed',
      stage: 'completed',
      progressMessage,
      decompressedZst: isZst,
      replayPrepared: kills.length > 0,
      kills,
      detectedPlayers,
      diagnostics,
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
      progressMessage: 'Ошибка при обработке файла демозаписи.',
      error: err?.message || 'Ошибка при обработке файла демозаписи на сервере.'
    });
  } finally {
    await cleanupFiles(filesToCleanup, jobId);
  }
};

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'cs2-kill-editor-server', time: ts() });
});

const handleStartAnalyzeJob = async (req, res) => {
  const uploadedFile = req.file;
  const playerNick = typeof req.body?.playerNick === 'string' ? req.body.playerNick.trim() : '';

  if (!uploadedFile) {
    logError('validation', new Error('Missing multipart file "demo"'), { reqId: req.reqId });
    return res.status(400).json({
      message: 'Не получен файл в поле multipart "demo".'
    });
  }

  if (!playerNick) {
    await cleanupFiles([uploadedFile.path], req.reqId);
    logError('validation', new Error('Missing required field "playerNick"'), { reqId: req.reqId });
    return res.status(400).json({
      message: 'Не указан обязательный параметр "playerNick".'
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
              message: `Размер файла превышает допустимый лимит (${Math.round(MAX_UPLOAD_BYTES / (1024 * 1024))} МБ).`
            });
          }
          return res.status(400).json({
            message: `Ошибка загрузки файла (${err.code}): ${err.message}`
          });
        }

        return res.status(err.statusCode || 400).json({
          message: err.message || 'Некорректный multipart/form-data запрос.'
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
      message: 'Задание не найдено. Возможно, оно истекло или сервер Render перезапускался.'
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
      message: 'Доступ к этому заданию запрещён.'
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
      detectedPlayers: job.detectedPlayers,
      diagnostics: job.diagnostics,
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
    message: err?.message || 'Внутренняя ошибка сервера.'
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
