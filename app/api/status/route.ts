import { NextRequest, NextResponse } from "next/server";

import { splitCsv } from "@/lib/coinalyze";
import { getCollectorEnv } from "@/lib/env";
import { prisma } from "@/lib/prisma";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const STALE_AFTER_MINUTES = 26 * 60;

function isAuthorized(request: NextRequest): boolean {
  const { COLLECTOR_BEARER_TOKEN } = getCollectorEnv();
  return request.headers.get("authorization") === `Bearer ${COLLECTOR_BEARER_TOKEN}`;
}

function minutesSince(now: Date, value: Date): number {
  return Math.max(0, Math.floor((now.getTime() - value.getTime()) / 60_000));
}

export async function GET(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const env = getCollectorEnv();
  const checkedAt = new Date();
  const symbols = splitCsv(env.COLLECTOR_DEFAULT_SYMBOLS);
  const timeframe = env.COLLECTOR_DEFAULT_TIMEFRAME;

  try {
    const [latestSuccess, latestBars] = await Promise.all([
      prisma.syncRun.findFirst({
        where: { status: "success" },
        orderBy: { finishedAt: "desc" },
      }),
      prisma.coinalyzeLiquidationBar.groupBy({
        by: ["symbol"],
        where: {
          symbol: { in: symbols },
          timeframe,
        },
        _max: { timestamp: true },
      }),
    ]);

    const latestBySymbol = new Map(
      latestBars.map((row) => [row.symbol, row._max.timestamp ?? null]),
    );

    const symbolFreshness = symbols.map((symbol) => {
      const latestBarAt = latestBySymbol.get(symbol) ?? null;
      const ageMinutes = latestBarAt ? minutesSince(checkedAt, latestBarAt) : null;
      return {
        symbol,
        latest_bar_at: latestBarAt?.toISOString() ?? null,
        age_minutes: ageMinutes,
        fresh: ageMinutes !== null && ageMinutes <= STALE_AFTER_MINUTES,
      };
    });

    const latestSuccessFinishedAt = latestSuccess?.finishedAt ?? null;
    const latestSuccessAgeMinutes = latestSuccessFinishedAt
      ? minutesSince(checkedAt, latestSuccessFinishedAt)
      : null;
    const staleSymbols = symbolFreshness.filter((row) => !row.fresh).map((row) => row.symbol);
    const healthy =
      staleSymbols.length === 0
      && latestSuccessAgeMinutes !== null
      && latestSuccessAgeMinutes <= STALE_AFTER_MINUTES;

    return NextResponse.json({
      ok: true,
      service: "coinalyze-collector",
      checked_at: checkedAt.toISOString(),
      timeframe,
      stale_after_minutes: STALE_AFTER_MINUTES,
      healthy,
      latest_success: latestSuccess
        ? {
            sync_run_id: latestSuccess.id.toString(),
            finished_at: latestSuccessFinishedAt?.toISOString() ?? null,
            rows_upserted: latestSuccess.rowsUpserted,
            window_to: latestSuccess.windowTo.toISOString(),
            note: latestSuccess.note ?? null,
            age_minutes: latestSuccessAgeMinutes,
          }
        : null,
      symbol_freshness: symbolFreshness,
      stale_symbols: staleSymbols,
    });
  } finally {
    await prisma.$disconnect();
  }
}