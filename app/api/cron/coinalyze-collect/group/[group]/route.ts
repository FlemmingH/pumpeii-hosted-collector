import { NextRequest, NextResponse } from "next/server";

import { handleCronRequest } from "../../handler";

export { dynamic, maxDuration, runtime } from "../../handler";

function parseGroupIndex(value: string): number | null {
  if (!/^\d+$/.test(value)) {
    return null;
  }

  const groupIndex = Number.parseInt(value, 10);
  return Number.isNaN(groupIndex) ? null : groupIndex;
}

async function handleGroupedRequest(
  request: NextRequest,
  context: { params: { group: string } },
) {
  const groupIndex = parseGroupIndex(context.params.group);
  if (groupIndex === null) {
    return NextResponse.json({ ok: false, error: "Invalid symbol group" }, { status: 404 });
  }

  return handleCronRequest(request, { groupIndex });
}

export async function GET(
  request: NextRequest,
  context: { params: { group: string } },
) {
  return handleGroupedRequest(request, context);
}

export async function POST(
  request: NextRequest,
  context: { params: { group: string } },
) {
  return handleGroupedRequest(request, context);
}