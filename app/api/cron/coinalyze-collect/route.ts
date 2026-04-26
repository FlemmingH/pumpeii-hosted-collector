import { NextRequest, NextResponse } from "next/server";

import { getCollectorEnv } from "@/lib/env";
import { runCollection } from "@/lib/collector";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";
export const maxDuration = 60;

function isAuthorized(request: NextRequest): boolean {
  const { CRON_SECRET } = getCollectorEnv();
  return request.headers.get("authorization") === `Bearer ${CRON_SECRET}`;
}

export async function POST(request: NextRequest) {
  if (!isAuthorized(request)) {
    return NextResponse.json({ ok: false, error: "Unauthorized" }, { status: 401 });
  }

  try {
    const result = await runCollection();
    return NextResponse.json({ ok: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Collector run failed";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}