/**
 * Shared ACLED API fetch with Redis caching.
 *
 * Three endpoints call ACLED independently (risk-scores, unrest-events,
 * acled-events) with overlapping queries. This shared layer ensures
 * identical queries hit Redis instead of making redundant upstream calls.
 */
import { CHROME_UA } from './constants';
import { cachedFetchJson } from './redis';

const ACLED_API_URL = 'https://acleddata.com/api/acled/read';
const ACLED_CACHE_TTL = 900; // 15 min — matches ACLED rate-limit window
const ACLED_TIMEOUT_MS = 15_000;

export interface AcledRawEvent {
  event_id_cnty?: string;
  event_type?: string;
  sub_event_type?: string;
  country?: string;
  location?: string;
  latitude?: string;
  longitude?: string;
  event_date?: string;
  fatalities?: string;
  source?: string;
  actor1?: string;
  actor2?: string;
  admin1?: string;
  notes?: string;
  tags?: string;
}

interface FetchAcledOptions {
  eventTypes: string;
  startDate: string;
  endDate: string;
  country?: string;
  limit?: number;
}

/**
 * Fetch ACLED events with automatic Redis caching.
 * Cache key is derived from query parameters so identical queries across
 * different handlers share the same cached result.
 */
export async function fetchAcledCached(opts: FetchAcledOptions): Promise<AcledRawEvent[]> {
  const token = process.env.ACLED_ACCESS_TOKEN;
  if (!token) {
    console.warn('[ACLED] ACLED_ACCESS_TOKEN not set — skipping');
    return [];
  }

  const cacheKey = `acled:shared:${opts.eventTypes}:${opts.startDate}:${opts.endDate}:${opts.country || 'all'}:${opts.limit || 500}`;
  const result = await cachedFetchJson<AcledRawEvent[]>(cacheKey, ACLED_CACHE_TTL, async () => {
    const params = new URLSearchParams({
      event_type: opts.eventTypes,
      event_date: `${opts.startDate}|${opts.endDate}`,
      event_date_where: 'BETWEEN',
      limit: String(opts.limit || 500),
      _format: 'json',
    });
    if (opts.country) params.set('country', opts.country);

    const url = `${ACLED_API_URL}?${params}`;
    console.log(`[ACLED] Fetching: ${opts.eventTypes} ${opts.startDate}..${opts.endDate} country=${opts.country || 'all'}`);

    const resp = await fetch(url, {
      headers: {
        Accept: 'application/json',
        Authorization: `Bearer ${token}`,
        'User-Agent': CHROME_UA,
      },
      signal: AbortSignal.timeout(ACLED_TIMEOUT_MS),
    });

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      console.error(`[ACLED] HTTP ${resp.status}: ${body.slice(0, 200)}`);
      throw new Error(`ACLED API error: ${resp.status} — ${body.slice(0, 100)}`);
    }
    const data = (await resp.json()) as { data?: AcledRawEvent[]; message?: string; error?: string };
    if (data.message || data.error) {
      console.error(`[ACLED] API error response: ${data.message || data.error}`);
      throw new Error(data.message || data.error || 'ACLED API error');
    }

    const events = data.data || [];
    console.log(`[ACLED] Got ${events.length} events for ${opts.eventTypes}`);
    return events.length > 0 ? events : null;
  });
  return result || [];
}
