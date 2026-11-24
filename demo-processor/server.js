import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { createClient } from '@supabase/supabase-js';
import { parseEvent, parseHeader } from '@laihoe/demoparser2';
import { decompress as zstdDecompress } from 'fzstd';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);

const PORT = Number(process.env.PORT) || 10000;
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://wuhhcamvnuvkgcobrjup.supabase.co';
const SUPABASE_ANON_KEY =
  process.env.SUPABASE_ANON_KEY ||
  process.env.SUPABASE_PUBLISHABLE_KEY ||
  'sb_publishable_M-Dlo8GKZ4TV_NwcPW8XuQ_7AyVBP5n';

const MAX_UPLOAD_BYTES = Number(process.env.MAX_UPLOAD_BYTES) || 800 * 1024 * 1024; // 800 МБ
const REQUEST_TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS) || 10 * 60 * 1000; // 10 мин

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

const logInfo = (stage, message, extra = {}) => {
  const payload = {
    time: ts(),
    stage,
    message,
    ...extra
  };
  console.log(`[${payload.time}][CS2-SERVER][${stage}] ${message}`, Object.keys(extra).length ? JSON.stringify(extra) : '');
};

const logError = (stage, err, extra = {}) => {
  const details = {
    time: ts(),
    stage,
    message: err?.message || String(err),
    code: err?.code,
    status: err?.status,
    stack: err?.stack,
    ...extra
  };
  console.error(`[${details.time}][CS2-SERVER][${stage}][ERROR]`, JSON.stringify(details, null, 2));
};

const cleanupFiles = async filePaths => {
  for (const filePath of filePaths) {
    if (!filePath) continue;
    try {
      await fs.promises.unlink(filePath);
    } catch (unlinkErr) {
      if (unlinkErr?.code !== 'ENOENT') {
        logError('cleanup', unlinkErr, { filePath });
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

// 1. Логирование получения HTTP-запроса: метод, URL и Origin
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

const decompressZstFile = async (zstPath, outDemPath, reqId) => {
  const cliAvailable = await hasZstdCli();
  if (cliAvailable) {
    logInfo('zstd-unpack', `Decompressing via zstd CLI: ${zstPath} -> ${outDemPath}`, { reqId });
    await execFileAsync('zstd', ['-d', '-f', '--no-progress', zstPath, '-o', outDemPath], {
      maxBuffer: 10 * 1024 * 1024
    });
    return;
  }

  logInfo('zstd-unpack', `Decompressing via fzstd library: ${zstPath} -> ${outDemPath}`, { reqId });
  const compressedBuffer = await fs.promises.readFile(zstPath);
  const compressedBytes = new Uint8Array(
    compressedBuffer.buffer,
    compressedBuffer.byteOffset,
    compressedBuffer.byteLength
  );
  const decompressedBytes = zstdDecompress(compressedBytes);
  if (!decompressedBytes || decompressedBytes.length < 16) {
    throw new Error('Распакованный поток Zstandard пуст или повреждён.');
  }
  await fs.promises.writeFile(outDemPath, Buffer.from(decompressedBytes));
};

const normalizeNick = value => String(value || '').trim().toLowerCase();

const parseKillsFromDemFile = async (demPath, playerNick, reqId) => {
  const stat = await fs.promises.stat(demPath);
  if (!stat.isFile() || stat.size < 16) {
    throw new Error('Распакованный файл .dem пуст или повреждён.');
  }

  const isSource2Demo = await hasMagicBytes(demPath, CS2_DEMO_MAGIC);
  if (!isSource2Demo) {
    throw new Error('Файл не содержит корректного заголовка CS2 Source 2 (PBDEMS2).');
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
    throw new Error('Парсер @laihoe/demoparser2 вернул некорректный формат событий player_death.');
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

app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'cs2-kill-editor-server', time: ts() });
});

const handleAnalyzeDemo = async (req, res) => {
  const filesToCleanup = [];
  const startedAt = Date.now();

  try {
    const uploadedFile = req.file;
    if (uploadedFile?.path) {
      filesToCleanup.push(uploadedFile.path);
    }

    const playerNick = typeof req.body?.playerNick === 'string' ? req.body.playerNick.trim() : '';

    if (!uploadedFile) {
      logError('validation', new Error('Missing multipart file "demo"'), { reqId: req.reqId });
      return res.status(400).json({
        message: 'Не получен файл в поле multipart "demo".'
      });
    }

    if (!playerNick) {
      logError('validation', new Error('Missing required field "playerNick"'), { reqId: req.reqId });
      return res.status(400).json({
        message: 'Не указан обязательный параметр "playerNick".'
      });
    }

    const originalName = uploadedFile.originalname || 'match.dem';
    const isZst = originalName.toLowerCase().endsWith('.dem.zst');
    let workingDemPath = uploadedFile.path;

    if (isZst) {
      // 4. Начало распаковки .dem.zst
      logInfo('decompress-start', `Starting .dem.zst decompression for "${originalName}" (${uploadedFile.size} bytes)`, {
        reqId: req.reqId,
        userId: req.supabaseUser.id,
        fileName: originalName,
        fileSize: uploadedFile.size
      });

      const validZst = await hasMagicBytes(uploadedFile.path, ZSTD_MAGIC);
      if (!validZst) {
        const invalidZstErr = new Error('Файл имеет расширение .dem.zst, но не содержит валидный заголовок Zstandard.');
        logError('decompress-invalid-magic', invalidZstErr, {
          reqId: req.reqId,
          fileName: originalName
        });
        return res.status(400).json({
          message: invalidZstErr.message
        });
      }

      const unpackedDemPath = path.join(tempUploadDir, `${path.basename(uploadedFile.path, '.zst')}_${Date.now()}.dem`);
      filesToCleanup.push(unpackedDemPath);

      const decompressStart = Date.now();
      await decompressZstFile(uploadedFile.path, unpackedDemPath, req.reqId);
      workingDemPath = unpackedDemPath;

      const unpackedStat = await fs.promises.stat(unpackedDemPath);
      // 5. Завершение распаковки
      logInfo('decompress-done', `Finished .dem.zst decompression -> "${path.basename(unpackedDemPath)}" (${unpackedStat.size} bytes) in ${Date.now() - decompressStart} ms`, {
        reqId: req.reqId,
        userId: req.supabaseUser.id,
        sourceFile: originalName,
        unpackedFile: path.basename(unpackedDemPath),
        unpackedSizeBytes: unpackedStat.size,
        durationMs: Date.now() - decompressStart
      });
    }

    // 6. Начало парсинга через @laihoe/demoparser2
    logInfo('parse-start', `Starting @laihoe/demoparser2 analysis for playerNick="${playerNick}"`, {
      reqId: req.reqId,
      userId: req.supabaseUser.id,
      fileName: originalName,
      playerNick
    });

    const parseStart = Date.now();
    const kills = await parseKillsFromDemFile(workingDemPath, playerNick, req.reqId);
    const parseDurationMs = Date.now() - parseStart;
    const elapsedMs = Date.now() - startedAt;

    // 7. Завершение парсинга и количество найденных убийств
    logInfo('parse-done', `Completed @laihoe/demoparser2 analysis: found ${kills.length} kills for "${playerNick}" in ${parseDurationMs} ms (total ${elapsedMs} ms)`, {
      reqId: req.reqId,
      userId: req.supabaseUser.id,
      nick: playerNick,
      file: originalName,
      killsCount: kills.length,
      parseDurationMs,
      totalElapsedMs: elapsedMs
    });

    return res.status(200).json({
      ok: true,
      playerNick,
      fileName: originalName,
      decompressedZst: isZst,
      replayPrepared: kills.length > 0,
      kills
    });
  } catch (err) {
    // 8. Все ошибки и тайм-ауты
    logError('analyze-demo', err, {
      reqId: req.reqId,
      userId: req.supabaseUser?.id,
      fileName: req.file?.originalname,
      fileSize: req.file?.size,
      playerNick: req.body?.playerNick,
      elapsedMs: Date.now() - startedAt
    });

    const status = Number.isInteger(err?.statusCode) ? err.statusCode : 500;
    return res.status(status).json({
      message: err?.message || 'Ошибка при обработке файла демозаписи на сервере.'
    });
  } finally {
    await cleanupFiles(filesToCleanup);
  }
};

app.post(
  '/api/v1/demos/analyze',
  requireSupabaseAuth,
  (req, res, next) => {
    // 2. Начало загрузки файла через Multer
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

      // 3. Завершение загрузки: имя и размер файла
      logInfo('multer-upload-done', `Multer upload completed: file="${req.file?.originalname || 'none'}", size=${req.file?.size ?? 0} bytes in ${Date.now() - uploadStart} ms`, {
        reqId: req.reqId,
        userId: req.supabaseUser?.id,
        fileName: req.file?.originalname || null,
        storedPath: req.file?.path || null,
        fileSizeBytes: req.file?.size ?? 0,
        playerNick: req.body?.playerNick || null,
        durationMs: Date.now() - uploadStart
      });

      return next();
    });
  },
  handleAnalyzeDemo
);

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
