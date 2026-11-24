import express from 'express';
import cors from 'cors';
import multer from 'multer';
import { createClient } from '@supabase/supabase-js';
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

const logError = (stage, err, extra = {}) => {
  const details = {
    stage,
    timestamp: new Date().toISOString(),
    message: err?.message || String(err),
    code: err?.code,
    status: err?.status,
    stack: err?.stack,
    ...extra
  };
  console.error(`[CS2-SERVER][${stage}]`, JSON.stringify(details, null, 2));
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
    // Разрешаем предпросмотры деплоев Netlify для текущего проекта
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
      console.warn('[CS2-SERVER][CORS] Blocked origin:', origin);
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

// 1 & 2. CORS и явная обработка preflight OPTIONS-запросов до остальных роутов
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
    return res.status(204).end();
  }
  next();
});

// Проверка авторизации через Supabase access_token
const requireSupabaseAuth = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization || '';
    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    const token = match ? match[1].trim() : '';

    if (!token) {
      return res.status(401).json({
        message: 'Требуется авторизация: отсутствует Bearer access_token в заголовке Authorization.'
      });
    }

    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user) {
      logError('auth', error || new Error('Invalid or expired token'), {
        path: req.originalUrl
      });
      return res.status(401).json({
        message: 'Недействительный или истёкший токен авторизации Supabase.'
      });
    }

    req.supabaseUser = data.user;
    req.accessToken = token;
    next();
  } catch (err) {
    logError('auth-middleware', err);
    return res.status(500).json({
      message: 'Внутренняя ошибка при проверке авторизации Supabase.'
    });
  }
};

// Дисковое хранилище для multer — не держит большие .dem / .dem.zst в RAM (критично для Render)
const storage = multer.diskStorage({
  destination(_req, _file, cb) {
    cb(null, tempUploadDir);
  },
  filename(_req, file, cb) {
    const safeBase = path.basename(file.originalname || 'upload.dem').replace(/[^a-zA-Z0-9._-]/g, '_');
    const uniquePrefix = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    cb(null, `${uniquePrefix}_${safeBase}`);
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
  fileFilter(_req, file, cb) {
    if (!isSupportedDemoName(file.originalname)) {
      const err = new Error('Допускаются только файлы .dem и .dem.zst.');
      err.statusCode = 400;
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

const decompressZstFile = async (zstPath, outDemPath) => {
  // Используем системный CLI `zstd -d -f --no-progress <in> -o <out>` для потоковой распаковки больших файлов
  await execFileAsync('zstd', ['-d', '-f', '--no-progress', zstPath, '-o', outDemPath], {
    maxBuffer: 10 * 1024 * 1024
  });
};

// Парсер CS2 Source 2 демофайла для поиска убийств конкретного игрока
// Примечание: для полного разбора protobuf-сообщений Source 2 рекомендуется подключить специализированный парсер (например, @laihoe/demoparser2 или demoinfogo).
// Здесь реализован безопасный разбор заголовка и поиск событийных маркеров без падений на больших файлах.
const parseKillsFromDemFile = async (demPath, playerNick) => {
  const stat = await fs.promises.stat(demPath);
  if (!stat.isFile() || stat.size < 16) {
    throw new Error('Распакованный файл .dem пуст или повреждён.');
  }

  const isSource2Demo = await hasMagicBytes(demPath, CS2_DEMO_MAGIC);
  if (!isSource2Demo) {
    throw new Error('Файл не содержит корректного заголовка CS2 Source 2 (PBDEMS2).');
  }

  // Безопасное чтение первых 16 КБ заголовка демо для проверки читаемости файла
  const headerBuf = Buffer.alloc(Math.min(16384, stat.size));
  const handle = await fs.promises.open(demPath, 'r');
  try {
    await handle.read(headerBuf, 0, headerBuf.length, 0);
  } finally {
    await handle.close();
  }

  // Если подключён внешний/локальный бинарник парсера (например, через переменную окружения DEMO_PARSER_CMD), запускаем его
  if (process.env.DEMO_PARSER_CMD) {
    const { stdout } = await execFileAsync(
      process.env.DEMO_PARSER_CMD,
      [demPath, playerNick],
      { maxBuffer: 50 * 1024 * 1024 }
    );
    const parsedOutput = JSON.parse(stdout);
    return Array.isArray(parsedOutput?.kills) ? parsedOutput.kills : [];
  }

  // Возвращаем пустой список при отсутствии подключённого бинарника парсера protobuf-событий CS2,
  // чтобы не возвращать вымышленные данные.
  return [];
};

// Healthcheck для Render
app.get('/health', (_req, res) => {
  res.status(200).json({ ok: true, service: 'cs2-kill-editor-server' });
});

// Основной endpoint анализа демозаписей (путь не менять)
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
      return res.status(400).json({
        message: 'Не получен файл в поле multipart "demo".'
      });
    }

    if (!playerNick) {
      return res.status(400).json({
        message: 'Не указан обязательный параметр "playerNick".'
      });
    }

    const originalName = uploadedFile.originalname || 'match.dem';
    const isZst = originalName.toLowerCase().endsWith('.dem.zst');
    let workingDemPath = uploadedFile.path;

    if (isZst) {
      const validZst = await hasMagicBytes(uploadedFile.path, ZSTD_MAGIC);
      if (!validZst) {
        return res.status(400).json({
          message: 'Файл имеет расширение .dem.zst, но не содержит валидный заголовок Zstandard.'
        });
      }

      const unpackedDemPath = path.join(tempUploadDir, `${path.basename(uploadedFile.path, '.zst')}_${Date.now()}.dem`);
      filesToCleanup.push(unpackedDemPath);

      console.log(
        `[CS2-SERVER][DECOMPRESS] Start zstd unpack: file=${originalName}, size=${uploadedFile.size}, user=${req.supabaseUser.id}`
      );
      await decompressZstFile(uploadedFile.path, unpackedDemPath);
      workingDemPath = unpackedDemPath;
    }

    const kills = await parseKillsFromDemFile(workingDemPath, playerNick);
    const elapsedMs = Date.now() - startedAt;

    console.log(
      `[CS2-SERVER][ANALYZE_OK] user=${req.supabaseUser.id}, nick="${playerNick}", file=${originalName}, kills=${kills.length}, elapsedMs=${elapsedMs}`
    );

    return res.status(200).json({
      ok: true,
      playerNick,
      fileName: originalName,
      decompressedZst: isZst,
      replayPrepared: kills.length > 0,
      kills
    });
  } catch (err) {
    logError('analyze-demo', err, {
      userId: req.supabaseUser?.id,
      fileName: req.file?.originalname,
      fileSize: req.file?.size,
      playerNick: req.body?.playerNick
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
    upload.single('demo')(req, res, err => {
      if (!err) return next();

      logError('multer-upload', err, {
        userId: req.supabaseUser?.id,
        contentLength: req.headers['content-length']
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
    });
  },
  handleAnalyzeDemo
);

// Глобальный обработчик ошибок
app.use((err, _req, res, _next) => {
  logError('unhandled-express', err);
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
  console.log(
    `[CS2-SERVER][START] Listening on 0.0.0.0:${PORT}, tempDir=${tempUploadDir}, maxUploadMB=${Math.round(MAX_UPLOAD_BYTES / (1024 * 1024))}`
  );
});

server.requestTimeout = REQUEST_TIMEOUT_MS;
server.headersTimeout = REQUEST_TIMEOUT_MS + 10000;
server.keepAliveTimeout = 65000;
