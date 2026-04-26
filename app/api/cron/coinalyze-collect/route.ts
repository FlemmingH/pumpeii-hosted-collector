import { NextRequest, NextResponse } from "next/server";

import { splitCsv } from "@/lib/coinalyze";
import { getCollectorEnv } from "@/lib/env";
import { runCollection } from "@/lib/collector";
import { prisma } from "@/lib/prisma";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

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

  const successes: Array<{ symbol: string; payload: Awaited<ReturnType<typeof runCollection>> }> = [];
  const failures: Array<{ symbol: string; status: number; error: unknown }> = [];

  for (const symbol of symbols) {
    try {
      const payload = await runCollection({ symbols: [symbol] });
      successes.push({ symbol, payload });
    } catch (error) {
      failures.push({
        symbol,
        status: 500,
        error: error instanceof Error ? error.message : String(error),
      });
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