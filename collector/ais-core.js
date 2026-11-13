/* ATLAS · collector/ais-core.js
 * Lógica pura del recolector AIS: interpreta mensajes de aisstream.io, fusiona con el estado
 * acumulado y genera el feed que lee ATLAS. Sin red ni disco: se prueba de forma aislada.
 *
 * Principios: solo reportes discretos, sin interpolar posiciones ni unir trayectorias.
 * La ausencia de reportes no se interpreta.
 */
'use strict';

const SCHEMA = 1;
const HOUR = 3600e3, DAY = 24 * HOUR;

/* Cajas de suscripción [[lat, lon], [lat, lon]], iguales a las vistas regionales de ATLAS. */
const REGIONS = {
  HORMUZ: [[24.3, 53.6], [27.9, 59.4]],
  SUEZ: [[26.4, 30.2], [32.6, 36.4]],
  MALACCA: [[-1.0, 94.5], [7.2, 106.4]],
};
const REGION_KEYS = Object.keys(REGIONS);

const LIMITS = {
  thinStateMs: 10 * 60e3,     // un reporte guardado cada 10 min por buque
  thinFeedMs: 30 * 60e3,      // un reporte publicado cada 30 min por buque
  reportsMs: DAY,             // historial de posiciones: 24 h
  tankerKeepMs: 30 * DAY,     // buques tanque vistos en los últimos 30 días
  unknownKeepMs: 7 * DAY,     // buques sin tipo confirmado
  registryKeepMs: 45 * DAY,   // datos estáticos
  seenKeepDays: 35,
  runsKeepMs: 62 * DAY,       // 30 días de ventana + 30 de comparación
};

const isTanker = (t) => Number.isInteger(t) && t >= 80 && t <= 89;
const num = (x) => (typeof x === 'number' && Number.isFinite(x) ? x : null);
const r4 = (x) => Math.round(x * 1e4) / 1e4;
const dayKey = (ms) => new Date(ms).toISOString().slice(0, 10);
const clean = (s) => (typeof s === 'string' ? s.replace(/@+/g, ' ').replace(/\s+/g, ' ').trim() : '');

function regionOf(lat, lon) {
  for (const k of REGION_KEYS) {
    const [[la1, lo1], [la2, lo2]] = REGIONS[k];
    if (lat >= Math.min(la1, la2) && lat <= Math.max(la1, la2) && lon >= Math.min(lo1, lo2) && lon <= Math.max(lo1, lo2)) return k;
  }
  return null;
}

/** "2026-11-13 08:12:44.123456 +0000 UTC" → ms. Si no se puede leer, se usa la hora de recepción. */
function parseTime(s, fallback) {
  if (typeof s === 'string' && /^\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d/.test(s)) {
    const ms = Date.parse(s.slice(0, 19).replace(' ', 'T') + 'Z');
    if (Number.isFinite(ms)) return ms;
  }
  return fallback;
}

function newState(now) {
  return { schema: SCHEMA, startedAt: now, registry: {}, vessels: {}, runs: [] };
}

function newRun(now) {
  const seen = {};
  REGION_KEYS.forEach((r) => { seen[r] = new Set(); });
  return { start: now, end: null, messages: 0, positions: 0, statics: 0, rejected: 0, seen, error: null };
}

/** Procesa un mensaje ya decodificado. Devuelve 'position', 'static', 'error' o 'ignored'. */
function processMessage(state, run, msg, now) {
  run.messages++;
  if (!msg || typeof msg !== 'object') return 'ignored';
  if (msg.error || msg.Error) { run.error = String(msg.error || msg.Error); return 'error'; }
  const meta = msg.MetaData || msg.Metadata || {};
  const body = msg.Message || {};
  const t = Math.min(parseTime(meta.time_utc, now), now);

  if (msg.MessageType === 'ShipStaticData' && body.ShipStaticData) {
    const s = body.ShipStaticData;
    const mmsi = num(meta.MMSI) || num(s.UserID);
    if (!mmsi) return 'ignored';
    run.statics++;
    const d = s.Dimension || {};
    const len = (num(d.A) || 0) + (num(d.B) || 0);
    state.registry[mmsi] = {
      type: Number.isInteger(s.Type) ? s.Type : null,
      name: clean(s.Name) || clean(meta.ShipName) || null,
      imo: num(s.ImoNumber) || null,
      dest: clean(s.Destination) || null,
      draught: num(s.MaximumStaticDraught) || null,
      len: len > 0 && len < 500 ? len : null,
      at: t,
    };
    return 'static';
  }

  if (msg.MessageType === 'PositionReport' && body.PositionReport) {
    const p = body.PositionReport;
    const mmsi = num(meta.MMSI) || num(p.UserID);
    if (!mmsi || p.Valid === false) { run.rejected++; return 'ignored'; }
    const lat = num(meta.latitude) !== null ? meta.latitude : num(p.Latitude);
    const lon = num(meta.longitude) !== null ? meta.longitude : num(p.Longitude);
    if (lat === null || lon === null || Math.abs(lat) > 90 || Math.abs(lon) > 180 || (lat === 0 && lon === 0)) { run.rejected++; return 'ignored'; }
    const region = regionOf(lat, lon);
    if (!region) { run.rejected++; return 'ignored'; }
    run.positions++;
    run.seen[region].add(mmsi);

    let sog = num(p.Sog); if (sog !== null && (sog >= 102.2 || sog < 0)) sog = null;
    let cog = num(p.Cog); if (cog !== null && (cog >= 360 || cog < 0)) cog = null;
    const nav = Number.isInteger(p.NavigationalStatus) ? p.NavigationalStatus : null;

    const v = state.vessels[mmsi] || (state.vessels[mmsi] = { lastSeen: 0, seen: {}, reports: [] });
    if (!v.name && clean(meta.ShipName)) v.name = clean(meta.ShipName);
    if (t >= v.lastSeen) Object.assign(v, { lastSeen: t, lat: r4(lat), lon: r4(lon), sog, cog, nav, region });
    const dk = dayKey(t);
    const days = v.seen[region] || (v.seen[region] = []);
    if (days.indexOf(dk) < 0) { days.push(dk); days.sort(); }
    const last = v.reports[v.reports.length - 1];
    if (!last || t - last[0] * 1000 >= LIMITS.thinStateMs) v.reports.push([Math.round(t / 1000), r4(lat), r4(lon), sog]);
    else if (t < last[0] * 1000) { /* reporte antiguo fuera de orden: se descarta */ }
    return 'position';
  }
  return 'ignored';
}

const typeOf = (state, mmsi) => (state.registry[mmsi] ? state.registry[mmsi].type : null);

/** Cierra la ejecución: recuentos por región y poda del estado. */
function finalizeRun(state, run, now) {
  run.end = now;
  const counts = {};
  REGION_KEYS.forEach((r) => {
    let tankers = 0, unknown = 0;
    run.seen[r].forEach((m) => { const t = typeOf(state, m); if (isTanker(t)) tankers++; else if (t === null) unknown++; });
    counts[r] = [tankers, unknown, run.seen[r].size];
  });
  state.runs.push({
    t: Math.round(run.start / 1000), d: Math.round((run.end - run.start) / 1000),
    m: run.messages, p: run.positions, s: run.statics, c: counts, e: run.error || undefined,
  });
  state.runs = state.runs.filter((x) => x.t * 1000 >= now - LIMITS.runsKeepMs);

  const minDay = dayKey(now - LIMITS.seenKeepDays * DAY);
  Object.keys(state.vessels).forEach((m) => {
    const v = state.vessels[m], t = typeOf(state, m);
    if (t !== null && !isTanker(t)) { delete state.vessels[m]; return; }
    if (isTanker(t)) {
      if (v.lastSeen < now - LIMITS.tankerKeepMs) { delete state.vessels[m]; return; }
      v.reports = v.reports.filter((x) => x[0] * 1000 >= now - LIMITS.reportsMs);
    } else {
      if (v.lastSeen < now - LIMITS.unknownKeepMs) { delete state.vessels[m]; return; }
      v.reports = v.reports.slice(-1);
    }
    Object.keys(v.seen).forEach((r) => { v.seen[r] = v.seen[r].filter((d) => d >= minDay); if (!v.seen[r].length) delete v.seen[r]; });
  });
  Object.keys(state.registry).forEach((m) => {
    const e = state.registry[m];
    if (e.at < now - LIMITS.registryKeepMs && !state.vessels[m]) { delete state.registry[m]; return; }
    if (!isTanker(e.type)) state.registry[m] = { type: e.type, at: e.at };
  });
  return counts;
}

function thin(reports, ms) {
  const out = [];
  reports.forEach((r) => { const last = out[out.length - 1]; if (!last || (r[0] - last[0]) * 1000 >= ms) out.push(r); });
  const tail = reports[reports.length - 1];
  if (tail && out[out.length - 1] !== tail) out.push(tail);
  return out;
}

/** Feed publicado: solo buques tanque confirmados por su tipo AIS (80-89). */
function buildFeed(state, now) {
  const vessels = [];
  Object.keys(state.vessels).forEach((m) => {
    const v = state.vessels[m], reg = state.registry[m];
    if (!reg || !isTanker(reg.type)) return;
    vessels.push({
      mmsi: Number(m), name: reg.name || v.name || null, imo: reg.imo, type: reg.type, dest: reg.dest, draught: reg.draught, len: reg.len,
      region: v.region, lastSeen: Math.round(v.lastSeen / 1000), lat: v.lat, lon: v.lon, sog: v.sog, cog: v.cog, nav: v.nav,
      seen: v.seen, reports: thin(v.reports.filter((x) => x[0] * 1000 >= now - LIMITS.reportsMs), LIMITS.thinFeedMs),
    });
  });
  vessels.sort((a, b) => b.lastSeen - a.lastSeen);
  const last = state.runs[state.runs.length - 1] || null;
  return {
    schema: SCHEMA, kind: 'atlas-ais-feed', source: 'aisstream.io',
    generatedAt: new Date(now).toISOString(), collectorStartedAt: new Date(state.startedAt).toISOString(),
    regions: REGIONS, vessels, runs: state.runs.map((x) => ({ t: x.t, d: x.d, c: x.c, e: x.e })),
    lastRun: last ? { start: new Date(last.t * 1000).toISOString(), seconds: last.d, messages: last.m, positions: last.p, statics: last.s, error: last.e || null } : null,
    notes: 'Buques tanque según tipo AIS 80-89 (crudo, productos, químicos y gas). Cobertura terrestre: la ausencia de reportes no implica ausencia de buques.',
  };
}

function feedScript(feed) {
  return '/* ATLAS AIS feed, generado ' + feed.generatedAt + '. Datos AIS reales de aisstream.io. */\natlasAisFeed(' + JSON.stringify(feed) + ');\n';
}

/** Recupera el estado desde JSON y restaura tipos. */
function loadState(json, now) {
  try {
    const s = JSON.parse(json);
    if (s && s.schema === SCHEMA && s.vessels && s.registry && Array.isArray(s.runs)) return s;
  } catch (_) { /* estado ilegible: se empieza de cero */ }
  return newState(now);
}

module.exports = { SCHEMA, REGIONS, REGION_KEYS, LIMITS, isTanker, regionOf, parseTime, newState, newRun, processMessage, finalizeRun, buildFeed, feedScript, loadState, dayKey };
