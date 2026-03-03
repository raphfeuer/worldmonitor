/**
 * Shared ACLED API fetch with Redis caching and automatic token refresh.
 *
 * Three endpoints call ACLED independently (risk-scores, unrest-events,
 * acled-events) with overlapping queries. This shared layer ensures
 * identical queries hit Redis instead of making redundant upstream calls.
 *
 * Token management:
 *   - If ACLED_EMAIL + ACLED_PASSWORD are set, auto-generates tokens via OAuth
 *   - If ACLED_REFRESH_TOKEN is set, refreshes expired access tokens
 *   - Falls back to static ACLED_ACCESS_TOKEN if set
 */
import { CHROME_UA } from './constants';
import { cachedFetchJson } from './redis';

const ACLED_API_URL = 'https://acleddata.com/api/acled/read';
const ACLED_OAUTH_URL = 'https://acleddata.com/oauth/token';
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

// In-memory token cache (per serverless invocation)
let cachedToken: string | null = null;
let tokenExpiresAt = 0;

/**
 * Get a valid ACLED access token, auto-refreshing if possible.
 *
 * Priority:
 *  1. In-memory cached token (if not expired)
 *  2. OAuth password grant (if ACLED_EMAIL + ACLED_PASSWORD set)
 *  3. Refresh token (if ACLED_REFRESH_TOKEN set)
 *  4. Static ACLED_ACCESS_TOKEN env var
 */
async function getAcledToken(): Promise<string | null> {
  // Return cached token if still valid (with 5-min buffer)
  if (cachedToken && Date.now() < tokenExpiresAt - 300_000) {
    return cachedToken;
  }

  // Try OAuth password grant
  const email = process.env.ACLED_EMAIL;
  const password = process.env.ACLED_PASSWORD;
  if (email && password) {
    try {
      const resp = await fetch(ACLED_OAUTH_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': CHROME_UA,
        },
        body: new URLSearchParams({
          username: email,
          password,
          grant_type: 'password',
          client_id: 'acled',
        }),
        signal: AbortSignal.timeout(10_000),
      });
      if (resp.ok) {
        const data = (await resp.json()) as {
          access_token?: string;
          expires_in?: number;
          refresh_token?: string;
        };
        if (data.access_token) {
          cachedToken = data.access_token;
          tokenExpiresAt = Date.now() + (data.expires_in || 86400) * 1000;
          console.log('[ACLED] Got fresh token via OAuth password grant');
          return cachedToken;
        }
      } else {
        console.error(`[ACLED] OAuth password grant failed: ${resp.status}`);
      }
    } catch (err) {
      console.error('[ACLED] OAuth password grant error:', err instanceof Error ? err.message : err);
    }
  }

  // Try refresh token
  const refreshToken = process.env.ACLED_REFRESH_TOKEN;
  if (refreshToken) {
    try {
      const resp = await fetch(ACLED_OAUTH_URL, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'User-Agent': CHROME_UA,
        },
        body: new URLSearchParams({
          refresh_token: refreshToken,
          grant_type: 'refresh_token',
          client_id: 'acled',
        }),
        signal: AbortSignal.timeout(10_000),
      });
      if (resp.ok) {
        const data = (await resp.json()) as {
          access_token?: string;
          expires_in?: number;
        };
        if (data.access_token) {
          cachedToken = data.access_token;
          tokenExpiresAt = Date.now() + (data.expires_in || 86400) * 1000;
          console.log('[ACLED] Got fresh token via refresh token');
          return cachedToken;
        }
      } else {
        console.error(`[ACLED] Refresh token failed: ${resp.status}`);
      }
    } catch (err) {
      console.error('[ACLED] Refresh token error:', err instanceof Error ? err.message : err);
    }
  }

  // Fall back to static token
  return process.env.ACLED_ACCESS_TOKEN || null;
}

/**
 * Fetch ACLED events with automatic Redis caching.
 * Cache key is derived from query parameters so identical queries across
 * different handlers share the same cached result.
 */
export async function fetchAcledCached(opts: FetchAcledOptions): Promise<AcledRawEvent[]> {
  const token = await getAcledToken();
  if (!token) {
    console.warn('[ACLED] No ACLED credentials available — skipping');
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
      // If 401/403, clear cached token so next call re-authenticates
      if (resp.status === 401 || resp.status === 403) {
        cachedToken = null;
        tokenExpiresAt = 0;
      }
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
