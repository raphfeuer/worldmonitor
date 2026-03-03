export const config = { runtime: 'edge' };

/**
 * Scrapes CONFLICT_ZONES from koala73/worldmonitor on GitHub,
 * parses the TypeScript source into JSON, returns to iOS app.
 * Cached 1h in-memory. Full autopilot — no manual sync needed.
 */

const GITHUB_RAW_URL =
  'https://raw.githubusercontent.com/koala73/worldmonitor/main/src/config/geo.ts';

let cached = null;
let cachedAt = 0;
const CACHE_TTL = 3600_000; // 1 hour

function parseConflictZones(ts) {
  const marker = ts.indexOf('export const CONFLICT_ZONES');
  if (marker === -1) return null;

  const eqIdx = ts.indexOf('= [', marker);
  if (eqIdx === -1) return null;
  const arrStart = eqIdx + 2; // point to '['

  // Bracket-match for the full array
  let depth = 0;
  let end = -1;
  for (let i = arrStart; i < ts.length; i++) {
    if (ts[i] === '[') depth++;
    else if (ts[i] === ']') { depth--; if (depth === 0) { end = i; break; } }
  }
  if (end === -1) return null;

  const raw = ts.slice(arrStart, end + 1);

  // Convert JS → JSON:
  // 1. Replace single quotes with double quotes
  // 2. Quote bare property keys (word at start of line before colon)
  // 3. Strip trailing commas before } or ]
  const json = raw
    .replace(/'/g, '"')
    .replace(/^\s*(\w+)\s*:/gm, '  "$1":')
    .replace(/,\s*([}\]])/g, '$1');

  try {
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function makeResponse(zones, status = 200, maxAge = 300) {
  return new Response(JSON.stringify({ zones }), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Cache-Control': `public, max-age=${maxAge}, s-maxage=3600, stale-if-error=3600`,
      'Access-Control-Allow-Origin': '*',
    },
  });
}

export default async function handler() {
  if (cached && Date.now() - cachedAt < CACHE_TTL) {
    return makeResponse(cached);
  }

  try {
    const resp = await fetch(GITHUB_RAW_URL, {
      headers: { 'User-Agent': 'WorldMonitor-iOS/1.0' },
      signal: AbortSignal.timeout(10_000),
    });
    if (!resp.ok) throw new Error(`GitHub ${resp.status}`);

    const ts = await resp.text();
    const zones = parseConflictZones(ts);
    if (!zones || zones.length === 0) throw new Error('Parse failed');

    cached = zones;
    cachedAt = Date.now();
    return makeResponse(zones);
  } catch (err) {
    if (cached) return makeResponse(cached, 200, 60);
    return new Response(JSON.stringify({ error: err.message, zones: [] }), {
      status: 502,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
    });
  }
}
