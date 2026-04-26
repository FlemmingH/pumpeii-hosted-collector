create schema if not exists collector;

create table if not exists collector.coinalyze_liquidation_bars (
  symbol text not null,
  timeframe text not null,
  timestamp timestamptz not null,
  long_liquidated_usd numeric not null default 0,
  short_liquidated_usd numeric not null default 0,
  long_liquidated_qty numeric not null default 0,
  short_liquidated_qty numeric not null default 0,
  liquidation_count integer not null default 0,
  source text not null default 'coinalyze_remote',
  market_count integer not null default 0,
  provider_markets jsonb not null default '[]'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (symbol, timeframe, timestamp)
);

create table if not exists collector.sync_runs (
  id bigint generated always as identity primary key,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null,
  window_from timestamptz not null,
  window_to timestamptz not null,
  rows_upserted integer not null default 0,
  note text
);