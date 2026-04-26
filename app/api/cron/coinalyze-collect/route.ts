import { NextRequest } from "next/server";

import { handleCronRequest } from "./handler";

export { dynamic, maxDuration, runtime } from "./handler";

export async function GET(request: NextRequest) {
  return handleCronRequest(request);
}

export async function POST(request: NextRequest) {
  return handleCronRequest(request);
}