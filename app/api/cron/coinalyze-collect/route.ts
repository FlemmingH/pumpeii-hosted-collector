import { NextRequest, NextResponse } from "next/server";

import { splitCsv } from "@/lib/coinalyze";
import { getCollectorEnv } from "@/lib/env";
import { runCollection } from "@/lib/collector";

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

async function fanOutBySymbol(request: NextRequest) {
  const env = getCollectorEnv();
  const symbols = splitCsv(env.COLLECTOR_DEFAULT_SYMBOLS);
  const authorization = `Bearer ${env.CRON_SECRET}`;

  const runs: PromiseSettledResult<{ symbol: string; status: number; payload: unknown }>[] = [];

  for (let index = 0; index < symbols.length; index += 2) {
    const group = symbols.slice(index, index + 2);
    const groupRuns = await Promise.allSettled(
      group.map(async (symbol) => {
      const url = new URL("/api/cron/coinalyze-collect", request.nextUrl.origin);
      url.searchParams.set("symbols", symbol);

      const response = await fetch(url, {
        method: "POST",
        headers: {
          authorization,
        },
        cache: "no-store",
      });

      let payload: unknown;

      try {
        payload = await response.json();
      } catch {
        payload = { ok: false, error: `Non-JSON response (${response.status})` };
      }

      return {
        symbol,
        status: response.status,
        payload,
      };
      }),
    );

    runs.push(...groupRuns);
  }

  const failures = runs.flatMap((run, index) => {
    const symbol = symbols[index];

    if (run.status === "rejected") {
      return [{ symbol, status: 500, error: run.reason instanceof Error ? run.reason.message : String(run.reason) }];
    }

    if (run.value.status >= 400) {
      return [{ symbol, status: run.value.status, error: run.value.payload }];
    }

    return [];
  });

  if (failures.length > 0) {
    return NextResponse.json(
      {
        ok: false,
        mode: "fanout",
        error: "One or more symbol workers failed",
        failures,
      },
      { status: 500 },
    );
  }

  const successes = runs
    .filter((run): run is PromiseFulfilledResult<{ symbol: string; status: number; payload: unknown }> => run.status === "fulfilled")
    .map((run) => run.value);

  const rowsUpserted = successes.reduce((total, run) => {
    if (typeof run.payload === "object" && run.payload !== null && "rows_upserted" in run.payload) {
      const rows = run.payload.rows_upserted;
      return total + (typeof rows === "number" ? rows : 0);
    }

    return total;
  }, 0);

  const marketsMatched = successes.reduce((total, run) => {
    if (typeof run.payload === "object" && run.payload !== null && "markets_matched" in run.payload) {
      const markets = run.payload.markets_matched;
      return total + (typeof markets === "number" ? markets : 0);
    }

    return total;
  }, 0);

  return NextResponse.json({
    ok: true,
    mode: "fanout",
    symbols,
    markets_matched: marketsMatched,
    rows_upserted: rowsUpserted,
    runs: successes.map((run) => run.payload),
  });
}

export async function POST(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  const requestedSymbols = getRequestedSymbols(request);

  try {
    if (requestedSymbols.length === 0) {
      return fanOutBySymbol(request);
    }

    const result = await runCollection({ symbols: requestedSymbols });
    return NextResponse.json({ ok: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Collector run failed";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}