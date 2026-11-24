import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import multer from 'multer';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { createClient } from '@supabase/supabase-js';
import { Decompress } from 'fzstd';
import { parseEvent } from '@laihoe/demoparser2';

const app = express();
const PORT = Number(process.env.PORT || 8080);
const UPLOAD_DIR = path.resolve(process.env.UPLOAD_DIR || './tmp_demos');
const MAX_UPLOAD_BYTES = 800 * 1024 * 1024; // 800 МБ
const MAX_UNPACKED_DEM_BYTES = 2 * 1024 * 1024 * 1024; // 2 ГБ защита от zip/zstd-бомб

const ZSTD_MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd]);
const CS2_DEMO_MAGIC = Buffer.from('PBDEMS2', 'ascii');

await fsp.mkdir(UPLOAD_DIR, { recursive: true });

// Supabase клиент только на сервере (использует переменные окружения, никогда не попадает в HTML)
const supabaseAdmin = process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY
  ? createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY)
  : null;

const allowedOrigins = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use(cors({
  origin: (origin, callback) => {
    if (!origin || allowedOrigins.length === 0 || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Origin запрещён политикой CORS'));
    }
  }
}));

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
  filename: (_req, file, cb) => {
    const safeExt = file.originalname.toLowerCase().endsWith('.dem.zst') ? '.dem.zst' : '.dem';
    cb(null, `${Date.now()}_${crypto.randomUUID()}${safeExt}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: MAX_UPLOAD_BYTES },
  fileFilter: (_req, file, cb) => {
    const lower = file.originalname.toLowerCase();
    if (!lower.endsWith('.dem') && !lower.endsWith('.dem.zst')) {
      return cb(new Error('Разрешены только файлы .dem и .dem.zst'));
    }
    cb(null, true);
  }
});

async function readFileHeader(filePath, length = 16) {
  const handle = await fsp.open(filePath, 'r');
  try {
    const buf = Buffer.alloc(length);
    const { bytesRead } = await handle.read(buf, 0, length, 0);
    return buf.subarray(0, bytesRead);
  } finally {
    await handle.close();
  }
}

// Потоковая распаковка Zstandard с диска на диск без загрузки всего архива в RAM
async function decompressZstStream(inputZstPath, outputDemPath) {
  const header = await readFileHeader(inputZstPath, 4);
  if (header.length < 4 || !header.subarray(0, 4).equals(ZSTD_MAGIC)) {
    throw new Error('Файл не является корректным Zstandard-архивом (.dem.zst).');
  }

  return new Promise((resolve, reject) => {
    const readStream = fs.createReadStream(inputZstPath);
    const writeStream = fs.createWriteStream(outputDemPath);
    let totalWritten = 0;
    let settled = false;

    const fail = err => {
      if (settled) return;
      settled = true;
      readStream.destroy();
      writeStream.destroy();
      reject(err);
    };

    const decompressor = new Decompress((chunk, _isLast) => {
      totalWritten += chunk.length;
      if (totalWritten > MAX_UNPACKED_DEM_BYTES) {
        fail(new Error('Превышен допустимый размер распакованного .dem файла (2 ГБ).'));
        return;
      }
      if (!writeStream.write(Buffer.from(chunk))) {
        readStream.pause();
        writeStream.once('drain', () => readStream.resume());
      }
    });

    readStream.on('data', chunk => {
      try {
        decompressor.push(new Uint8Array(chunk));
      } catch (err) {
        fail(new Error(`Ошибка распаковки повреждённого архива Zstandard: ${err.message}`));
      }
    });

    readStream.on('end', () => {
      try {
        decompressor.push(new Uint8Array(0), true);
        writeStream.end();
      } catch (err) {
        fail(new Error(`Незавершённый или повреждённый поток Zstandard: ${err.message}`));
      }
    });

    readStream.on('error', fail);
    writeStream.on('error', fail);
    writeStream.on('finish', () => {
      if (!settled) {
        settled = true;
        resolve(totalWritten);
      }
    });
  });
}

async function verifyCs2DemoHeader(demPath) {
  const header = await readFileHeader(demPath, 16);
  if (header.length < CS2_DEMO_MAGIC.length || !header.subarray(0, CS2_DEMO_MAGIC.length).equals(CS2_DEMO_MAGIC)) {
    throw new Error('Распакованный файл не является валидной демозаписью CS2 Source 2 (отсутствует заголовок PBDEMS2).');
  }
}

async function safeUnlink(filePath) {
  if (!filePath) return;
  try {
    await fsp.unlink(filePath);
  } catch {
    // Игнорируем, если файл уже удалён
  }
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, service: 'cs2-kill-editor-demo-server' });
});

app.post('/api/v1/demos/analyze', upload.single('demo'), async (req, res) => {
  const uploadedPath = req.file?.path || null;
  let unpackedDemPath = null;

  try {
    if (!req.file) {
      return res.status(400).json({ message: 'Файл .dem или .dem.zst не передан.' });
    }

    const playerNick = String(req.body?.playerNick || '').trim();
    if (!playerNick) {
      return res.status(400).json({ message: 'Не указан игровой никнейм.' });
    }

    // Проверка токена пользователя Supabase (если передан и настроен Supabase)
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    if (supabaseAdmin && token) {
      const { data, error } = await supabaseAdmin.auth.getUser(token);
      if (error || !data?.user) {
        return res.status(401).json({ message: 'Недействительная сессия пользователя.' });
      }
    }

    const isZst = req.file.originalname.toLowerCase().endsWith('.dem.zst') || req.body?.compression === 'zstd';
    let targetDemPath = uploadedPath;

    if (isZst) {
      unpackedDemPath = `${uploadedPath}.unpacked.dem`;
      await decompressZstStream(uploadedPath, unpackedDemPath);
      targetDemPath = unpackedDemPath;
    }

    // Проверка сигнатуры CS2 demo перед передачей парсеру
    await verifyCs2DemoHeader(targetDemPath);

    // Парсинг событий убийств CS2 demo
    const deathEvents = parseEvent(targetDemPath, 'player_death', ['tick', 'total_rounds_played'], ['name']);
    const normalizedTarget = playerNick.toLowerCase();

    const kills = (Array.isArray(deathEvents) ? deathEvents : [])
      .filter(ev => String(ev.attacker_name || '').trim().toLowerCase() === normalizedTarget)
      .map(ev => {
        const tick = Number(ev.tick || 0);
        const seconds = Math.floor(tick / 64);
        const mm = String(Math.floor(seconds / 60)).padStart(2, '0');
        const ss = String(seconds % 60).padStart(2, '0');
        return {
          round: Number(ev.total_rounds_played || 0) + 1,
          tick,
          matchTime: `${mm}:${ss}`,
          weapon: ev.weapon || '',
          victim: ev.user_name || '',
          headshot: Boolean(ev.headshot),
          replayReady: false
        };
      });

    return res.json({
      ok: true,
      sourceFormat: isZst ? 'dem.zst' : 'dem',
      playerNick,
      killsCount: kills.length,
      kills,
      replayPrepared: false
    });
  } catch (err) {
    return res.status(400).json({
      message: err?.message || 'Ошибка при распаковке или парсинге демозаписи.'
    });
  } finally {
    await safeUnlink(uploadedPath);
    await safeUnlink(unpackedDemPath);
  }
});

app.use((err, _req, res, _next) => {
  res.status(400).json({ message: err?.message || 'Ошибка загрузки файла.' });
});

app.listen(PORT, () => {
  console.log(`CS2 Demo Processor listening on port ${PORT}`);
});
