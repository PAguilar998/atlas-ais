/* ATLAS · collector/test-collector.js
 * Pruebas del recolector: lógica pura y una ejecución real contra un servidor WebSocket local
 * que imita el protocolo de aisstream.io. Uso: node collector/test-collector.js
 */
'use strict';
const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const http = require('http');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const core = require('./ais-core.js');

const results = [];
async function test(name, fn) {
  try { await fn(); results.push([true, name]); } catch (e) { results.push([false, name + ' :: ' + e.message]); }
}

const T0 = Date.parse('2026-11-13T08:00:00Z');
const fmt = (ms) => new Date(ms).toISOString().replace('T', ' ').replace('Z', '') + '000 +0000 UTC';
const pos = (mmsi, lat, lon, ms, extra) => ({
  MessageType: 'PositionReport',
  MetaData: { MMSI: mmsi, ShipName: 'SHIP ' + mmsi + '@@@', latitude: lat, longitude: lon, time_utc: fmt(ms) },
  Message: { PositionReport: Object.assign({ UserID: mmsi, Latitude: lat, Longitude: lon, Sog: 11.2, Cog: 120.5, TrueHeading: 121, NavigationalStatus: 0, Valid: true }, extra || {}) },
});
const stat = (mmsi, type, ms) => ({
  MessageType: 'ShipStaticData',
  MetaData: { MMSI: mmsi, ShipName: 'SHIP ' + mmsi, latitude: 26.5, longitude: 56.3, time_utc: fmt(ms) },
  Message: { ShipStaticData: { UserID: mmsi, Type: type, Name: 'SHIP ' + mmsi + '@@', ImoNumber: 9000000 + (mmsi % 1000), Destination: 'FUJAIRAH@@', MaximumStaticDraught: 14.2, Dimension: { A: 200, B: 50, C: 20, D: 20 }, Valid: true } },
});

(async () => {
  await test('Regiones: Ormuz, Suez y Malaca; fuera de cajas devuelve null', () => {
    assert.strictEqual(core.regionOf(26.5, 56.4), 'HORMUZ');
    assert.strictEqual(core.regionOf(30.5, 32.3), 'SUEZ');
    assert.strictEqual(core.regionOf(1.2, 103.8), 'MALACCA');
    assert.strictEqual(core.regionOf(40, 10), null);
  });
  await test('Hora de aisstream: formato con microsegundos y zona', () => {
    assert.strictEqual(core.parseTime('2026-11-13 08:12:44.318353 +0000 UTC', 0), Date.parse('2026-11-13T08:12:44Z'));
    assert.strictEqual(core.parseTime('basura', 42), 42);
  });
  await test('Tipo AIS: solo 80-89 son buques tanque', () => {
    assert.ok(core.isTanker(80) && core.isTanker(89));
    assert.ok(!core.isTanker(79) && !core.isTanker(90) && !core.isTanker(null));
  });

  await test('Fusión: posiciones, datos estáticos y feed solo con buques tanque', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    core.processMessage(s, run, stat(111, 80, T0), T0);
    core.processMessage(s, run, stat(222, 70, T0), T0);           // carguero
    core.processMessage(s, run, pos(111, 26.5, 56.4, T0), T0);
    core.processMessage(s, run, pos(222, 26.4, 56.5, T0), T0);
    core.processMessage(s, run, pos(333, 1.2, 103.9, T0), T0);    // sin tipo
    const c = core.finalizeRun(s, run, T0 + 240e3);
    assert.deepStrictEqual(c.HORMUZ, [1, 0, 2]);
    assert.deepStrictEqual(c.MALACCA, [0, 1, 1]);
    const f = core.buildFeed(s, T0 + 240e3);
    assert.strictEqual(f.vessels.length, 1);
    assert.strictEqual(f.vessels[0].mmsi, 111);
    assert.strictEqual(f.vessels[0].name, 'SHIP 111');
    assert.strictEqual(f.vessels[0].dest, 'FUJAIRAH');
    assert.strictEqual(f.vessels[0].len, 250);
    assert.ok(!s.vessels[222], 'el carguero debe salir del estado');
    assert.ok(s.vessels[333], 'el buque sin tipo se conserva por si llega su tipo');
  });

  await test('Un tipo conocido después convierte a buque tanque a uno observado antes', () => {
    const s = core.newState(T0);
    let run = core.newRun(T0);
    core.processMessage(s, run, pos(444, 1.3, 103.7, T0), T0);
    core.finalizeRun(s, run, T0 + 60e3);
    assert.strictEqual(core.buildFeed(s, T0 + 60e3).vessels.length, 0);
    run = core.newRun(T0 + 1800e3);
    core.processMessage(s, run, stat(444, 84, T0 + 1800e3), T0 + 1800e3);
    core.finalizeRun(s, run, T0 + 1900e3);
    const f = core.buildFeed(s, T0 + 1900e3);
    assert.strictEqual(f.vessels.length, 1);
    assert.strictEqual(f.vessels[0].region, 'MALACCA');
  });

  await test('Rechazos: inválido, 0,0, fuera de región y sentinelas de velocidad y rumbo', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    core.processMessage(s, run, pos(1, 26.5, 56.4, T0, { Valid: false }), T0);
    core.processMessage(s, run, pos(2, 0, 0, T0), T0);
    core.processMessage(s, run, pos(3, 45, 10, T0), T0);
    const m = pos(4, 91, 181, T0); m.MetaData.latitude = 91; m.MetaData.longitude = 181;
    core.processMessage(s, run, m, T0);
    core.processMessage(s, run, pos(5, 26.5, 56.4, T0, { Sog: 102.3, Cog: 360 }), T0);
    assert.strictEqual(run.rejected, 4);
    assert.strictEqual(s.vessels[5].sog, null);
    assert.strictEqual(s.vessels[5].cog, null);
  });

  await test('Sin interpolación: un reporte guardado cada 10 min como máximo', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    for (let k = 0; k < 30; k++) core.processMessage(s, run, pos(7, 26.5 + k * 0.001, 56.4, T0 + k * 60e3), T0 + k * 60e3);
    assert.strictEqual(s.vessels[7].reports.length, 3);
    s.vessels[7].reports.forEach((r) => assert.ok(Math.abs(r[1] - 26.5) < 0.05));
  });

  await test('Error del servidor (clave no válida) se registra', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    assert.strictEqual(core.processMessage(s, run, { error: 'Api Key Is Not Valid' }, T0), 'error');
    core.finalizeRun(s, run, T0 + 1000);
    const f = core.buildFeed(s, T0 + 1000);
    assert.strictEqual(f.lastRun.error, 'Api Key Is Not Valid');
    assert.strictEqual(f.runs[0].e, 'Api Key Is Not Valid');
  });

  await test('Poda: buque tanque no visto en 30 días desaparece; ejecuciones de más de 62 días también', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    core.processMessage(s, run, stat(9, 80, T0), T0);
    core.processMessage(s, run, pos(9, 26.5, 56.4, T0), T0);
    core.finalizeRun(s, run, T0);
    const later = T0 + 31 * 86400e3;
    core.finalizeRun(s, core.newRun(later), later);
    assert.strictEqual(core.buildFeed(s, later).vessels.length, 0);
    const much = T0 + 70 * 86400e3;
    core.finalizeRun(s, core.newRun(much), much);
    assert.strictEqual(s.runs.length, 2);
    assert.ok(s.runs.every((x) => x.t * 1000 > T0), 'la ejecución inicial debe haberse podado');
  });

  await test('El feed se publica como llamada atlasAisFeed y el estado sobrevive a JSON', () => {
    const s = core.newState(T0), run = core.newRun(T0);
    core.processMessage(s, run, stat(5, 81, T0), T0);
    core.processMessage(s, run, pos(5, 30.5, 32.33, T0), T0);
    core.finalizeRun(s, run, T0 + 1);
    const js = core.feedScript(core.buildFeed(s, T0 + 1));
    let got = null;
    new Function('atlasAisFeed', js)((x) => { got = x; });
    assert.strictEqual(got.vessels[0].region, 'SUEZ');
    const s2 = core.loadState(JSON.stringify(s), T0);
    assert.strictEqual(Object.keys(s2.vessels).length, 1);
    assert.strictEqual(core.loadState('{roto', T0).runs.length, 0);
  });

  /* ---------- Servidor WebSocket mínimo (RFC 6455) que imita aisstream ---------- */
  function frame(payload, opcode) {
    const len = payload.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | opcode, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
    return Buffer.concat([head, payload]);
  }
  function readFrame(buf) {
    if (buf.length < 2) return null;
    let len = buf[1] & 127, off = 2;
    if (len === 126) { len = buf.readUInt16BE(2); off = 4; } else if (len === 127) { len = Number(buf.readBigUInt64BE(2)); off = 10; }
    const masked = buf[1] & 128;
    const mask = masked ? buf.slice(off, off + 4) : null;
    off += masked ? 4 : 0;
    if (buf.length < off + len) return null;
    const data = Buffer.from(buf.slice(off, off + len));
    if (mask) for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4];
    return { opcode: buf[0] & 15, data, rest: buf.slice(off + len) };
  }
  function mockServer(script) {
    const log = { subscription: null, connections: 0 };
    const srv = http.createServer();
    srv.on('upgrade', (req, sock) => {
      log.connections++;
      const accept = crypto.createHash('sha1').update(req.headers['sec-websocket-key'] + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
      sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
      let buf = Buffer.alloc(0);
      sock.on('data', (d) => {
        buf = Buffer.concat([buf, d]);
        let f;
        while ((f = readFrame(buf))) {
          buf = f.rest;
          if (f.opcode === 8) { try { sock.end(frame(Buffer.alloc(0), 8)); } catch (_) { /* cerrado */ } return; }
          if (f.opcode === 1 && !log.subscription) { log.subscription = JSON.parse(f.data.toString()); script(sock, log.subscription, (m, binary) => sock.write(frame(Buffer.from(JSON.stringify(m)), binary ? 2 : 1))); }
        }
      });
      sock.on('error', () => {});
    });
    return new Promise((ok) => srv.listen(0, '127.0.0.1', () => ok({ srv, log, port: srv.address().port })));
  }
  function runCollector(port, env, dir) {
    return new Promise((ok) => {
      const p = spawn(process.execPath, [path.join(__dirname, 'collect.js'), '--state', path.join(dir, 'state.json'), '--out', path.join(dir, 'site')], {
        env: Object.assign({}, process.env, { AISSTREAM_URL: 'ws://127.0.0.1:' + port, DURATION_SEC: '10' }, env),
      });
      let out = '';
      p.stdout.on('data', (d) => { out += d; }); p.stderr.on('data', (d) => { out += d; });
      p.on('close', (code) => ok({ code, out }));
    });
  }

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'atlas-ais-'));
  await test('Ejecución real: suscripción correcta, mensajes binarios y de texto, feed escrito', async () => {
    const now = Date.now();
    const { srv, log, port } = await mockServer((sock, sub, send) => {
      send(stat(311000001, 80, now), true);
      send(pos(311000001, 26.55, 56.45, now), true);
      send(stat(311000002, 89, now), false);
      send(pos(311000002, 1.25, 103.8, now), false);
      send(pos(311000003, 30.6, 32.32, now), true);
      setTimeout(() => sock.end(frame(Buffer.alloc(0), 8)), 300);
    });
    const r = await runCollector(port, { AISSTREAM_API_KEY: 'test-key-123' }, tmp);
    srv.close();
    assert.strictEqual(r.code, 0, r.out);
    assert.strictEqual(log.subscription.APIKey, 'test-key-123');
    assert.deepStrictEqual(log.subscription.FilterMessageTypes, ['PositionReport', 'ShipStaticData']);
    assert.strictEqual(log.subscription.BoundingBoxes.length, 3);
    assert.deepStrictEqual(log.subscription.BoundingBoxes[0], [[24.3, 53.6], [27.9, 59.4]]);
    const js = fs.readFileSync(path.join(tmp, 'site', 'ais-live.js'), 'utf8');
    assert.ok(js.indexOf('test-key-123') < 0, 'la clave no debe aparecer en el feed');
    assert.ok(fs.readFileSync(path.join(tmp, 'state.json'), 'utf8').indexOf('test-key-123') < 0, 'la clave no debe aparecer en el estado');
    let feed = null; new Function('atlasAisFeed', js)((x) => { feed = x; });
    assert.strictEqual(feed.vessels.length, 2);
    assert.deepStrictEqual(feed.runs[0].c.SUEZ, [0, 1, 1]);
  });

  await test('Segunda ejecución acumula sobre el estado anterior', async () => {
    const now = Date.now();
    const { srv, port } = await mockServer((sock, sub, send) => {
      send(stat(311000003, 83, now), true);
      send(pos(311000003, 30.62, 32.32, now), true);
      setTimeout(() => sock.end(frame(Buffer.alloc(0), 8)), 200);
    });
    const r = await runCollector(port, { AISSTREAM_API_KEY: 'k' }, tmp);
    srv.close();
    assert.strictEqual(r.code, 0, r.out);
    let feed = null; new Function('atlasAisFeed', fs.readFileSync(path.join(tmp, 'site', 'ais-live.js'), 'utf8'))((x) => { feed = x; });
    assert.strictEqual(feed.vessels.length, 3);
    assert.strictEqual(feed.runs.length, 2);
  });

  await test('Clave rechazada: termina con error, conserva el estado y lo anota en el feed', async () => {
    const { srv, port } = await mockServer((sock, sub, send) => {
      send({ error: 'Api Key Is Not Valid' }, false);
      setTimeout(() => sock.end(frame(Buffer.alloc(0), 8)), 100);
    });
    const r = await runCollector(port, { AISSTREAM_API_KEY: 'mala' }, tmp);
    srv.close();
    assert.strictEqual(r.code, 1);
    let feed = null; new Function('atlasAisFeed', fs.readFileSync(path.join(tmp, 'site', 'ais-live.js'), 'utf8'))((x) => { feed = x; });
    assert.strictEqual(feed.vessels.length, 3, 'no debe perder lo acumulado');
    assert.strictEqual(feed.lastRun.error, 'Api Key Is Not Valid');
  });

  await test('Sin clave: no se conecta y explica dónde configurarla', async () => {
    const r = await runCollector(1, { AISSTREAM_API_KEY: '' }, tmp);
    assert.strictEqual(r.code, 2);
    assert.ok(/AISSTREAM_API_KEY/.test(r.out));
  });

  await test('Servidor inaccesible: error de conexión sin bloquearse', async () => {
    const r = await runCollector(9, { AISSTREAM_API_KEY: 'k' }, tmp);
    assert.strictEqual(r.code, 1, r.out);
  });

  results.forEach(([ok, n]) => { if (!ok) console.log('FALLA  ' + n); });
  const ok = results.filter((r) => r[0]).length;
  console.log(ok + '/' + results.length + ' pruebas del recolector correctas');
  fs.rmSync(tmp, { recursive: true, force: true });
  process.exit(ok === results.length ? 0 : 1);
})();
