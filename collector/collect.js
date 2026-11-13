#!/usr/bin/env node
/* ATLAS · collector/collect.js
 * Escucha aisstream.io durante DURATION_SEC segundos, fusiona con el estado previo y publica el feed.
 * Requiere Node 22 o superior (WebSocket nativo). Sin dependencias.
 *
 *   AISSTREAM_API_KEY=xxxx node collector/collect.js --state state/ais-state.json --out site
 *
 * La clave se lee solo de la variable de entorno y nunca se escribe en disco ni en el feed.
 */
'use strict';
const fs = require('fs');
const path = require('path');
const core = require('./ais-core.js');

function arg(name, def) {
  const i = process.argv.indexOf('--' + name);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : def;
}

const KEY = process.env.AISSTREAM_API_KEY || '';
const URL = process.env.AISSTREAM_URL || 'wss://stream.aisstream.io/v0/stream';
const DURATION = Math.max(10, Number(process.env.DURATION_SEC || 240)) * 1000;
const STATE = arg('state', 'state/ais-state.json');
const OUT = arg('out', 'site');

function writeAtomic(file, text) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file + '.tmp', text);
  fs.renameSync(file + '.tmp', file);
}

function indexPage(feed) {
  const n = feed.vessels.length;
  const err = feed.lastRun && feed.lastRun.error ? '<p>Último intento con error: ' + String(feed.lastRun.error).replace(/[<>&]/g, '') + '</p>' : '';
  return '<!doctype html><meta charset="utf-8"><title>ATLAS AIS feed</title><body style="font:14px system-ui;padding:24px;background:#060a12;color:#dce4ee">' +
    '<h1 style="font-size:18px">ATLAS AIS feed</h1><p>Actualizado ' + feed.generatedAt + '. ' + n + ' buques tanque en los últimos 30 días.</p>' + err +
    '<p>URL para ATLAS: <code>' + 'ais-live.js' + '</code> en esta misma dirección.</p><p>Datos AIS de aisstream.io. Cobertura terrestre.</p></body>';
}

async function main() {
  if (!KEY) {
    console.error('Falta AISSTREAM_API_KEY. En GitHub: Settings > Secrets and variables > Actions > New repository secret.');
    process.exit(2);
  }
  if (typeof WebSocket === 'undefined') {
    console.error('Este Node no tiene WebSocket nativo. Usa Node 22 o superior.');
    process.exit(2);
  }
  const t0 = Date.now();
  const state = fs.existsSync(STATE) ? core.loadState(fs.readFileSync(STATE, 'utf8'), t0) : core.newState(t0);
  const run = core.newRun(t0);
  const decoder = new TextDecoder();

  await new Promise((resolve) => {
    let done = false;
    const finish = (why) => { if (done) return; done = true; clearTimeout(timer); console.log('Fin de la escucha: ' + why); try { ws.close(); } catch (_) { /* ya cerrado */ } resolve(); };
    const ws = new WebSocket(URL);
    ws.binaryType = 'arraybuffer';
    const timer = setTimeout(() => finish('duración alcanzada'), DURATION);
    ws.onopen = () => {
      ws.send(JSON.stringify({
        APIKey: KEY,
        BoundingBoxes: core.REGION_KEYS.map((r) => core.REGIONS[r]),
        FilterMessageTypes: ['PositionReport', 'ShipStaticData'],
      }));
      console.log('Conectado. Escuchando ' + DURATION / 1000 + ' s en ' + core.REGION_KEYS.join(', ') + '.');
    };
    ws.onmessage = (ev) => {
      let msg;
      try { msg = JSON.parse(typeof ev.data === 'string' ? ev.data : decoder.decode(ev.data)); } catch (_) { run.messages++; return; }
      if (core.processMessage(state, run, msg, Date.now()) === 'error') finish('error del servidor: ' + run.error);
    };
    ws.onerror = () => { if (!run.error) run.error = 'Error de conexión con ' + URL; finish('error de conexión'); };
    ws.onclose = (ev) => {
      if (!done && !run.error && run.messages === 0) run.error = 'Conexión cerrada sin datos (código ' + ev.code + '). Revisa la clave.';
      finish('conexión cerrada');
    };
  });

  const now = Date.now();
  const counts = core.finalizeRun(state, run, now);
  const feed = core.buildFeed(state, now);
  writeAtomic(STATE, JSON.stringify(state));
  writeAtomic(path.join(OUT, 'ais-live.js'), core.feedScript(feed));
  writeAtomic(path.join(OUT, 'ais-live.json'), JSON.stringify(feed));
  writeAtomic(path.join(OUT, 'index.html'), indexPage(feed));

  console.log('Mensajes ' + run.messages + ', posiciones ' + run.positions + ', estáticos ' + run.statics + ', descartados ' + run.rejected + '.');
  core.REGION_KEYS.forEach((r) => console.log('  ' + r + ': ' + counts[r][0] + ' buques tanque, ' + counts[r][1] + ' sin tipo confirmado, ' + counts[r][2] + ' buques en total.'));
  console.log('Feed: ' + feed.vessels.length + ' buques tanque en 30 días, ' + Math.round(fs.statSync(path.join(OUT, 'ais-live.js')).size / 1024) + ' KB.');
  if (run.error) { console.error('Error: ' + run.error); process.exit(1); }
}

main().catch((e) => { console.error(e); process.exit(1); });
