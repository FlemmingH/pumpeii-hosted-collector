# Coinalyze Collector

Standalone Vercel and Supabase collector for Phase 1 of the hosted
Coinalyze plan.

This repo is intentionally separate from the local Pumpeii runtime.

This app does one job:

- fetch a rolling 5-day Coinalyze liquidation window for the six-symbol Pumpeii universe
- aggregate matching stable perpetual markets into normalized 5m rows
- upsert those rows into Supabase Postgres under the `collector` schema
- expose a Vercel cron target at `/api/cron/coinalyze-collect`

## Setup

1. Copy `.env.example` to `.env.local`.
2. Run the SQL in `supabase/collector_schema.sql` in Supabase SQL Editor, or use `pnpm prisma:push` after setting the database URLs.
3. Install dependencies with `pnpm install`.
4. Generate the Prisma client with `pnpm prisma:generate`.
5. Start locally with `pnpm dev`.

## Endpoints

- `POST /api/cron/coinalyze-collect`
- `GET /api/health`

The cron route requires `Authorization: Bearer <CRON_SECRET>`.

## Notes

- Vercel Hobby only allows one cron execution per day, so `vercel.json` is configured for a daily run. Use the protected cron route manually, or upgrade the project plan, if you need higher-frequency collection.
- The hosted defaults use a 1-day overlap window and the cron route runs each tracked symbol sequentially within the same invocation, which keeps the full job under the Vercel timeout budget without opening extra database sessions.
- This is Phase 1 only. It does not implement the local Pumpeii importer.
- Supabase remains upstream storage only. Backtests and runtime code should continue to read local Postgres.