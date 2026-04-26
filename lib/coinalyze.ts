import Decimal from "decimal.js";

export const COINALYZE_BASE_URL = "https://api.coinalyze.net/v1";

const INTERVALS: Record<string, string> = {
  "1m": "1min",
  "5m": "5min",
  "15m": "15min",
  "30m": "30min",
  "1h": "1hour",
  "2h": "2hour",
  "4h": "4hour",
  "6h": "6hour",
  "12h": "12hour",
  "1d": "daily",
};

export type CoinalyzeMarket = {
  symbol: string;
  exchange: string;
  baseAsset: string;
  quoteAsset: string;
  isPerpetual: boolean;
  margined: string;
  pumpeiiSymbol: string;
  providerLabel: string;
};

type CoinalyzeHistoryPoint = {
  t: number;
  l?: number | string;
  s?: number | string;
};

type CoinalyzeHistoryItem = {
  symbol: string;
  history: CoinalyzeHistoryPoint[];
};

type AggregatedBarInternal = {
  symbol: string;
  timeframe: string;
  timestamp: Date;
  longLiquidatedUsd: Decimal;
  shortLiquidatedUsd: Decimal;
  longLiquidatedQty: Decimal;
  shortLiquidatedQty: Decimal;
  liquidationCount: number;
  source: string;
  marketCount: number;
  providerMarkets: string[];
};

type AggregatedBarMutable = Omit<AggregatedBarInternal, "providerMarkets" | "marketCount"> & {
  providerMarkets: Set<string>;
};

export type AggregatedBar = {
  symbol: string;
  timeframe: string;
  timestamp: Date;
  longLiquidatedUsd: Decimal;
  shortLiquidatedUsd: Decimal;
  longLiquidatedQty: Decimal;
  shortLiquidatedQty: Decimal;
  liquidationCount: number;
  source: string;
  marketCount: number;
  providerMarkets: string[];
};

export type CoinalyzeRequestOptions = {
  maxRetries: number;
  retryBaseDelayMs: number;
  timeoutMs: number;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function sleep(delayMs: number): Promise<void> {
  if (delayMs <= 0) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

function parseDecimal(value: unknown): Decimal {
  try {
    return new Decimal(String(value ?? 0));
  } catch (error) {
    throw new Error(`Invalid Coinalyze decimal value: ${String(value)}`, { cause: error });
  }
}

function marketFromPayload(value: unknown): CoinalyzeMarket | null {
  if (!isRecord(value) || typeof value.symbol !== "string" || value.symbol.length === 0) {
    return null;
  }

  const baseAsset = String(value.base_asset ?? "").toUpperCase();
  const quoteAsset = String(value.quote_asset ?? "").toUpperCase();
  const exchange = String(value.exchange ?? "").toUpperCase();
  const symbol = value.symbol;

  return {
    symbol,
    exchange,
    baseAsset,
    quoteAsset,
    isPerpetual: Boolean(value.is_perpetual),
    margined: String(value.margined ?? "").toUpperCase(),
    pumpeiiSymbol: `${baseAsset}/${quoteAsset}`,
    providerLabel: `${exchange}:${symbol}`,
  };
}

function normalizeHistoryItem(value: unknown): CoinalyzeHistoryItem | null {
  if (!isRecord(value) || typeof value.symbol !== "string") {
    return null;
  }
  if (!Array.isArray(value.history)) {
    return null;
  }
  return {
    symbol: value.symbol,
    history: value.history.filter(isRecord).map((point) => ({
      t: Number(point.t ?? 0),
      l: point.l as number | string | undefined,
      s: point.s as number | string | undefined,
    })),
  };
}

async function fetchJson(path: string, apiKey: string, params?: URLSearchParams): Promise<unknown> {
  return fetchJsonWithRetry(path, apiKey, { maxRetries: 0, retryBaseDelayMs: 0, timeoutMs: 30000 }, params);
}

function parseRetryAfterMs(headerValue: string | null): number | null {
  if (!headerValue) {
    return null;
  }

  const seconds = Number(headerValue);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return seconds * 1000;
  }

  const retryDate = Date.parse(headerValue);
  if (Number.isNaN(retryDate)) {
    return null;
  }

  return Math.max(0, retryDate - Date.now());
}

async function fetchJsonWithRetry(
  path: string,
  apiKey: string,
  requestOptions: CoinalyzeRequestOptions,
  params?: URLSearchParams,
): Promise<unknown> {
  const url = params ? `${COINALYZE_BASE_URL}${path}?${params.toString()}` : `${COINALYZE_BASE_URL}${path}`;
  const maxRetries = Math.max(0, requestOptions.maxRetries);

  for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
    try {
      const response = await fetch(url, {
        headers: {
          api_key: apiKey,
        },
        cache: "no-store",
        signal: AbortSignal.timeout(requestOptions.timeoutMs),
      });

      if (response.ok) {
        return response.json();
      }

      const shouldRetry = response.status === 429 || response.status >= 500;
      if (!shouldRetry || attempt === maxRetries) {
        throw new Error(`Coinalyze request failed: ${response.status} ${response.statusText}`);
      }

      const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
      const backoffMs = retryAfterMs ?? requestOptions.retryBaseDelayMs * (attempt + 1);
      await sleep(backoffMs);
    } catch (error) {
      if (attempt === maxRetries) {
        throw error;
      }

      const message = error instanceof Error ? error.message : String(error);
      const retryableError = message.includes("429") || message.includes("fetch failed");
      if (!retryableError) {
        throw error;
      }

      await sleep(requestOptions.retryBaseDelayMs * (attempt + 1));
    }
  }

  throw new Error(`Coinalyze request failed: exhausted retries for ${path}`);
}

export function splitCsv(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

export function defaultWindow(days: number): { start: Date; end: Date } {
  const end = new Date();
  end.setUTCSeconds(0, 0);
  const start = new Date(end);
  start.setUTCDate(start.getUTCDate() - Math.max(1, days));
  return { start, end };
}

export function timeframeToInterval(timeframe: string): string {
  const interval = INTERVALS[timeframe];
  if (!interval) {
    throw new Error(`Unsupported Coinalyze liquidation timeframe: ${timeframe}`);
  }
  return interval;
}

export function selectMarkets(
  markets: readonly CoinalyzeMarket[],
  symbols: readonly string[],
): Record<string, CoinalyzeMarket[]> {
  const selected = new Map<string, CoinalyzeMarket[]>();
  const byUpper = new Map(symbols.map((symbol) => [symbol.toUpperCase(), symbol]));

  for (const symbol of symbols) {
    selected.set(symbol, []);
  }

  for (const market of markets) {
    if (!market.isPerpetual || market.margined !== "STABLE") {
      continue;
    }
    const wanted = byUpper.get(market.pumpeiiSymbol.toUpperCase());
    if (!wanted) {
      continue;
    }
    selected.get(wanted)?.push(market);
  }

  return Object.fromEntries(
    Array.from(selected.entries()).filter(([, value]) => value.length > 0),
  );
}

export function chunk<T>(items: readonly T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += Math.max(1, size)) {
    chunks.push(items.slice(index, index + Math.max(1, size)));
  }
  return chunks;
}

export async function fetchFutureMarkets(
  apiKey: string,
  requestOptions: CoinalyzeRequestOptions,
): Promise<CoinalyzeMarket[]> {
  const payload = await fetchJsonWithRetry("/future-markets", apiKey, requestOptions);
  if (!Array.isArray(payload)) {
    throw new Error("Coinalyze future-markets response must be a list");
  }

  return payload
    .map(marketFromPayload)
    .filter((market): market is CoinalyzeMarket => market !== null);
}

export async function fetchLiquidationHistory(args: {
  apiKey: string;
  markets: readonly CoinalyzeMarket[];
  timeframe: string;
  start: Date;
  end: Date;
  requestOptions: CoinalyzeRequestOptions;
}): Promise<CoinalyzeHistoryItem[]> {
  const params = new URLSearchParams({
    symbols: args.markets.map((market) => market.symbol).join(","),
    interval: timeframeToInterval(args.timeframe),
    from: String(Math.floor(args.start.getTime() / 1000)),
    to: String(Math.floor(args.end.getTime() / 1000)),
    convert_to_usd: "true",
  });

  const payload = await fetchJsonWithRetry(
    "/liquidation-history",
    args.apiKey,
    args.requestOptions,
    params,
  );
  if (!Array.isArray(payload)) {
    throw new Error("Coinalyze liquidation-history response must be a list");
  }

  return payload
    .map(normalizeHistoryItem)
    .filter((item): item is CoinalyzeHistoryItem => item !== null);
}

export function coinalyzeHistoryToBars(args: {
  payload: readonly CoinalyzeHistoryItem[];
  symbolByCoinalyzeSymbol: Readonly<Record<string, string>>;
  providerByCoinalyzeSymbol: Readonly<Record<string, string>>;
  timeframe: string;
  source: string;
}): AggregatedBar[] {
  const aggregated = new Map<string, AggregatedBarMutable>();

  for (const item of args.payload) {
    const symbol = args.symbolByCoinalyzeSymbol[item.symbol];
    const providerMarket = args.providerByCoinalyzeSymbol[item.symbol];
    if (!symbol || !providerMarket) {
      continue;
    }

    for (const point of item.history) {
      const timestamp = new Date(point.t * 1000);
      const key = `${symbol}|${args.timeframe}|${timestamp.toISOString()}`;
      const existing = aggregated.get(key) ?? {
        symbol,
        timeframe: args.timeframe,
        timestamp,
        longLiquidatedUsd: new Decimal(0),
        shortLiquidatedUsd: new Decimal(0),
        longLiquidatedQty: new Decimal(0),
        shortLiquidatedQty: new Decimal(0),
        liquidationCount: 0,
        source: args.source,
        providerMarkets: new Set<string>(),
      };

      existing.longLiquidatedUsd = existing.longLiquidatedUsd.plus(parseDecimal(point.l));
      existing.shortLiquidatedUsd = existing.shortLiquidatedUsd.plus(parseDecimal(point.s));
      existing.providerMarkets.add(providerMarket);
      aggregated.set(key, existing);
    }
  }

  return Array.from(aggregated.values())
    .map((row): AggregatedBarInternal => ({
      ...row,
      marketCount: row.providerMarkets.size,
      providerMarkets: Array.from(row.providerMarkets).sort(),
    }))
    .sort((left, right) => {
      if (left.symbol === right.symbol) {
        return left.timestamp.getTime() - right.timestamp.getTime();
      }
      return left.symbol.localeCompare(right.symbol);
    });
}

export function mergeBars(rows: readonly AggregatedBar[]): AggregatedBar[] {
  const merged = new Map<string, AggregatedBarMutable>();

  for (const row of rows) {
    const key = `${row.symbol}|${row.timeframe}|${row.timestamp.toISOString()}`;
    const existing = merged.get(key) ?? {
      symbol: row.symbol,
      timeframe: row.timeframe,
      timestamp: row.timestamp,
      longLiquidatedUsd: new Decimal(0),
      shortLiquidatedUsd: new Decimal(0),
      longLiquidatedQty: new Decimal(0),
      shortLiquidatedQty: new Decimal(0),
      liquidationCount: 0,
      source: row.source,
      providerMarkets: new Set<string>(),
    };

    existing.longLiquidatedUsd = existing.longLiquidatedUsd.plus(row.longLiquidatedUsd);
    existing.shortLiquidatedUsd = existing.shortLiquidatedUsd.plus(row.shortLiquidatedUsd);
    existing.longLiquidatedQty = existing.longLiquidatedQty.plus(row.longLiquidatedQty);
    existing.shortLiquidatedQty = existing.shortLiquidatedQty.plus(row.shortLiquidatedQty);
    existing.liquidationCount += row.liquidationCount;
    for (const providerMarket of row.providerMarkets) {
      existing.providerMarkets.add(providerMarket);
    }
    merged.set(key, existing);
  }

  return Array.from(merged.values())
    .map((row): AggregatedBarInternal => ({
      ...row,
      marketCount: row.providerMarkets.size,
      providerMarkets: Array.from(row.providerMarkets).sort(),
    }))
    .sort((left, right) => {
      if (left.symbol === right.symbol) {
        return left.timestamp.getTime() - right.timestamp.getTime();
      }
      return left.symbol.localeCompare(right.symbol);
    });
}