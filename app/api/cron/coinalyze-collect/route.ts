import { NextRequest, NextResponse } from "next/server";

import { chunk, splitCsv } from "@/lib/coinalyze";
import { getCollectorEnv } from "@/lib/env";
import { runCollection } from "@/lib/collector";
import { prisma } from "@/lib/prisma";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

const SYMBOL_GROUP_SIZE = 2;

function sleep(delayMs: number): Promise<void> {
  if (delayMs <= 0) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

function isAuthorized(request: NextRequest): boolean {
  const { CRON_SECRET } = getCollectorEnv();
  return request.headers.get("authorization") === `Bearer ${CRON_SECRET}`;
}

function getRequestedSymbols(request: NextRequest): string[] {
  const raw = request.nextUrl.searchParams.get("symbols");
  return raw ? splitCsv(raw) : [];
}

async function fanOutBySymbol() {
  const env = getCollectorEnv();
  const symbols = splitCsv(env.COLLECTOR_DEFAULT_SYMBOLS);
  const symbolGroups = chunk(symbols, SYMBOL_GROUP_SIZE);

  const successes: Array<{ symbols: string[]; payload: Awaited<ReturnType<typeof runCollection>> }> = [];
  const failures: Array<{ symbols: string[]; status: number; error: unknown }> = [];

  for (const [index, symbolGroup] of symbolGroups.entries()) {
    const groupStartedAt = Date.now();

    try {
      const payload = await runCollection({ symbols: symbolGroup });
      console.info(
        JSON.stringify({
          scope: "coinalyze_collector",
          event: "symbol_group_complete",
          symbols: symbolGroup,
          duration_ms: Date.now() - groupStartedAt,
          markets_matched: payload.markets_matched,
          rows_upserted: payload.rows_upserted,
        }),
      );
      successes.push({ symbols: symbolGroup, payload });
    } catch (error) {
      console.error(
        JSON.stringify({
          scope: "coinalyze_collector",
          event: "symbol_group_failed",
          symbols: symbolGroup,
          duration_ms: Date.now() - groupStartedAt,
          error: error instanceof Error ? error.message : String(error),
        }),
      );
      failures.push({
        symbols: symbolGroup,
        status: 500,
        error: error instanceof Error ? error.message : String(error),
      });
    }

    if (index < symbolGroups.length - 1) {
      await sleep(env.COLLECTOR_BATCH_DELAY_MS);
    }
  }

  if (failures.length > 0) {
    return NextResponse.json(
      {
        ok: false,
        mode: "fanout",
        error: "One or more symbol runs failed",
        failures,
      },
      { status: 500 },
    );
  }

  const rowsUpserted = successes.reduce((total, run) => {
    return total + run.payload.rows_upserted;
  }, 0);

  const marketsMatched = successes.reduce((total, run) => {
    return total + run.payload.markets_matched;
  }, 0);

  return NextResponse.json({
    ok: true,
    mode: "fanout",
    symbols,
    markets_matched: marketsMatched,
    rows_upserted: rowsUpserted,
    runs: successes.map((run) => ({ ok: true, ...run.payload })),
  });
}

export async function POST(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const requestedSymbols = getRequestedSymbols(request);

  try {
    if (requestedSymbols.length === 0) {
      return await fanOutBySymbol();
    }

    const result = await runCollection({ symbols: requestedSymbols });
    return NextResponse.json({ ok: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Collector run failed";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  } finally {
    await prisma.$disconnect();
  }
}