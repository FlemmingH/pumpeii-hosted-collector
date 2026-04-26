import { prisma } from "@/lib/prisma";
import {
  chunk,
  coinalyzeHistoryToBars,
  defaultWindow,
  fetchFutureMarkets,
  fetchLiquidationHistory,
  mergeBars,
  selectMarkets,
  splitCsv,
} from "@/lib/coinalyze";
import { getCollectorEnv } from "@/lib/env";

const FUTURE_MARKETS_TTL_MS = 60_000;

let futureMarketsCache:
  | {
      fetchedAt: number;
      markets: Awaited<ReturnType<typeof fetchFutureMarkets>>;
    }
  | undefined;

function logCollectorEvent(event: string, payload: Record<string, unknown>) {
  console.info(
    JSON.stringify({
      scope: "coinalyze_collector",
      event,
      ...payload,
    }),
  );
}

function sleep(delayMs: number): Promise<void> {
  if (delayMs <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

async function upsertBars(rows: ReturnType<typeof mergeBars>): Promise<number> {
  const batches = chunk(rows, 250);

  for (const batch of batches) {
    await prisma.$transaction(
      batch.map((row) =>
        prisma.coinalyzeLiquidationBar.upsert({
          where: {
            symbol_timeframe_timestamp: {
              symbol: row.symbol,
              timeframe: row.timeframe,
              timestamp: row.timestamp,
            },
          },
          create: {
            symbol: row.symbol,
            timeframe: row.timeframe,
            timestamp: row.timestamp,
            longLiquidatedUsd: row.longLiquidatedUsd.toString(),
            shortLiquidatedUsd: row.shortLiquidatedUsd.toString(),
            longLiquidatedQty: row.longLiquidatedQty.toString(),
            shortLiquidatedQty: row.shortLiquidatedQty.toString(),
            liquidationCount: row.liquidationCount,
            source: row.source,
            marketCount: row.marketCount,
            providerMarkets: row.providerMarkets,
          },
          update: {
            longLiquidatedUsd: row.longLiquidatedUsd.toString(),
            shortLiquidatedUsd: row.shortLiquidatedUsd.toString(),
            longLiquidatedQty: row.longLiquidatedQty.toString(),
            shortLiquidatedQty: row.shortLiquidatedQty.toString(),
            liquidationCount: row.liquidationCount,
            source: row.source,
            marketCount: row.marketCount,
            providerMarkets: row.providerMarkets,
          },
        }),
      ),
    );
  }

  return rows.length;
}

async function fetchLiquidationHistoryAdaptive(args: {
  apiKey: string;
  markets: Parameters<typeof fetchLiquidationHistory>[0]["markets"];
  timeframe: string;
  start: Date;
  end: Date;
  requestOptions: Parameters<typeof fetchLiquidationHistory>[0]["requestOptions"];
  delayMs: number;
}): Promise<Awaited<ReturnType<typeof fetchLiquidationHistory>>> {
  try {
    return await fetchLiquidationHistory({
      apiKey: args.apiKey,
      markets: args.markets,
      timeframe: args.timeframe,
      start: args.start,
      end: args.end,
      requestOptions: {
        ...args.requestOptions,
        maxRetries: 0,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!message.includes("429") || args.markets.length <= 1) {
      throw error;
    }

    logCollectorEvent("liquidation_history_split", {
      market_count: args.markets.length,
      timeframe: args.timeframe,
      reason: message,
    });

    const midpoint = Math.ceil(args.markets.length / 2);
    const leftMarkets = args.markets.slice(0, midpoint);
    const rightMarkets = args.markets.slice(midpoint);

    const leftPayload = await fetchLiquidationHistoryAdaptive({
      ...args,
      markets: leftMarkets,
    });

    await sleep(args.delayMs);

    const rightPayload = await fetchLiquidationHistoryAdaptive({
      ...args,
      markets: rightMarkets,
    });

    return [...leftPayload, ...rightPayload];
  }
}

async function getFutureMarketsCached(args: {
  apiKey: string;
  requestOptions: Parameters<typeof fetchFutureMarkets>[1];
}) {
  const now = Date.now();

  if (futureMarketsCache && now - futureMarketsCache.fetchedAt < FUTURE_MARKETS_TTL_MS) {
    return futureMarketsCache.markets;
  }

  const markets = await fetchFutureMarkets(args.apiKey, args.requestOptions);
  futureMarketsCache = {
    fetchedAt: now,
    markets,
  };
  return markets;
}

export async function runCollection(options?: { symbols?: string[] }) {
  const env = getCollectorEnv();
  const symbols = options?.symbols?.length
    ? options.symbols
    : splitCsv(env.COLLECTOR_DEFAULT_SYMBOLS);
  const startedAt = Date.now();
  const timeframe = env.COLLECTOR_DEFAULT_TIMEFRAME;
  const { start, end } = defaultWindow(env.COLLECTOR_OVERLAP_DAYS);
  const requestOptions = {
    maxRetries: env.COINALYZE_MAX_RETRIES,
    retryBaseDelayMs: env.COINALYZE_RETRY_BASE_DELAY_MS,
    timeoutMs: env.COINALYZE_REQUEST_TIMEOUT_MS,
  };

  const syncRun = await prisma.syncRun.create({
    data: {
      status: "running",
      windowFrom: start,
      windowTo: end,
    },
  });

  try {
    const allMarkets = await getFutureMarketsCached({
      apiKey: env.COINALYZE_API_KEY,
      requestOptions,
    });
    const selectedBySymbol = selectMarkets(allMarkets, symbols);
    const selectedMarkets = Object.values(selectedBySymbol).flat();

    if (selectedMarkets.length === 0) {
      throw new Error("No matching Coinalyze perpetual markets found");
    }

    const symbolByCoinalyzeSymbol = Object.fromEntries(
      selectedMarkets.map((market) => [market.symbol, market.pumpeiiSymbol]),
    );
    const providerByCoinalyzeSymbol = Object.fromEntries(
      selectedMarkets.map((market) => [market.symbol, market.providerLabel]),
    );

    const batches = chunk(selectedMarkets, env.COLLECTOR_BATCH_SIZE);
    const rows = [];

    for (const [index, batch] of batches.entries()) {
      const payload = await fetchLiquidationHistoryAdaptive({
        apiKey: env.COINALYZE_API_KEY,
        markets: batch,
        timeframe,
        start,
        end,
        requestOptions,
        delayMs: env.COLLECTOR_BATCH_DELAY_MS,
      });

      rows.push(
        ...coinalyzeHistoryToBars({
          payload,
          symbolByCoinalyzeSymbol,
          providerByCoinalyzeSymbol,
          timeframe,
          source: "coinalyze_remote",
        }),
      );

      if (index < batches.length - 1) {
        await sleep(env.COLLECTOR_BATCH_DELAY_MS);
      }
    }

    const mergedRows = mergeBars(rows);
    const rowsUpserted = await upsertBars(mergedRows);

    logCollectorEvent("run_complete", {
      duration_ms: Date.now() - startedAt,
      symbols,
      markets_matched: selectedMarkets.length,
      rows_upserted: rowsUpserted,
    });

    await prisma.syncRun.update({
      where: { id: syncRun.id },
      data: {
        finishedAt: new Date(),
        status: "success",
        rowsUpserted,
        note: `markets=${selectedMarkets.length}`,
      },
    });

    return {
      window_from: start.toISOString(),
      window_to: end.toISOString(),
      timeframe,
      symbols,
      markets_matched: selectedMarkets.length,
      rows_upserted: rowsUpserted,
      sync_run_id: syncRun.id.toString(),
    };
  } catch (error) {
    const note = error instanceof Error ? error.message : "Collector run failed";

    logCollectorEvent("run_failed", {
      duration_ms: Date.now() - startedAt,
      symbols,
      error: note,
    });

    await prisma.syncRun.update({
      where: { id: syncRun.id },
      data: {
        finishedAt: new Date(),
        status: "failed",
        note,
      },
    });

    throw error;
  }
}