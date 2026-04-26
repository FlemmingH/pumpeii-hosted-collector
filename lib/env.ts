import "server-only";

import { z } from "zod";

const defaultSymbols = "BTC/USDT,ETH/USDT,SOL/USDT,LINK/USDT,AVAX/USDT,XRP/USDT";

const collectorEnvSchema = z.object({
  COINALYZE_API_KEY: z.string().min(1),
  COLLECTOR_BEARER_TOKEN: z.string().min(1),
  COLLECTOR_DEFAULT_SYMBOLS: z.string().default(defaultSymbols),
  COLLECTOR_DEFAULT_TIMEFRAME: z.string().default("5m"),
  COLLECTOR_OVERLAP_DAYS: z.coerce.number().int().positive().default(1),
  COLLECTOR_BATCH_SIZE: z.coerce.number().int().positive().default(20),
  COLLECTOR_BATCH_DELAY_MS: z.coerce.number().int().nonnegative().default(2000),
  COINALYZE_MAX_RETRIES: z.coerce.number().int().nonnegative().default(4),
  COINALYZE_RETRY_BASE_DELAY_MS: z.coerce.number().int().nonnegative().default(2000),
  COINALYZE_REQUEST_TIMEOUT_MS: z.coerce.number().int().positive().default(30000),
  CRON_SECRET: z.string().min(1),
});

const supabaseAdminEnvSchema = z.object({
  SUPABASE_URL: z.string().url(),
  SUPABASE_SERVICE_ROLE_KEY: z.string().min(1),
});

export type CollectorEnv = z.infer<typeof collectorEnvSchema>;
export type SupabaseAdminEnv = z.infer<typeof supabaseAdminEnvSchema>;

export function getCollectorEnv(): CollectorEnv {
  return collectorEnvSchema.parse(process.env);
}

export function getSupabaseAdminEnv(): SupabaseAdminEnv {
  return supabaseAdminEnvSchema.parse(process.env);
}

export const DEFAULT_COLLECTOR_SYMBOLS = defaultSymbols;