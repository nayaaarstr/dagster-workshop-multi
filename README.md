# pipeline_weather

Ingests current weather observations (temperature, wind speed) for five
Indonesian cities from the free [Open-Meteo](https://open-meteo.com/) API and
lands them in a `weather_observations` table. Picked weather because it's a
public, no-auth API that updates continuously, so the pipeline has fresh data
to materialize on every run — a good fit for demonstrating Dagster's daily
schedule pattern.

Built on top of [dagster-workshop-multi](https://github.com/<original-org>/dagster-workshop-multi),
a multi-container Dagster workshop — see that repo's README for the base
architecture (`pipeline_products`, `pipeline_fx`, `pipeline_ml`).

## What I built

- **Track:** A — new source pipeline
- **Data source:** [Open-Meteo Forecast API](https://open-meteo.com/en/docs) (no API key required)
- **Key assets:**
  - `raw_weather` — fetches current weather for 5 cities (Jakarta, Bandung,
    Surabaya, Yogyakarta, Medan) from Open-Meteo
  - `weather_table` — loads `raw_weather` into the shared warehouse Postgres
    as `weather_observations`
- **Quality gate:** `temperature_in_plausible_range` — an `@asset_check` on
  `raw_weather` that fails if any reading falls outside -90°C to 60°C
  (Earth's recorded temperature extremes), catching bad API responses or
  parsing bugs before they land in the warehouse.

## Architecture

```
                     dagster_webserver (:3000)  <-- workspace.yaml -->  dagster_daemon
                              |                                              |
                              +---------------------+-----------------------+
                                                     |
                             dagster_postgresql  (Dagster's own run/schedule/event storage)

  pipeline_products (:4000)      pipeline_fx (:4001)      pipeline_ml (:4002)      pipeline_weather (:4003)
  fakestoreapi.com ->            api.frankfurter.app ->   trains a classifier      api.open-meteo.com ->
  raw_products/raw_orders        raw_exchange_rates       on products+orders       raw_weather
        |                              |                        |                        |
        v                              v                        v                        v
  products, orders  ----------->  warehouse_postgresql  <---------------------------------+
  tables                          (also: exchange_rates,
                                   order_value_predictions,
                                   weather_observations)
```

## Running it

```bash
docker compose up --build
```

Open http://localhost:3000, find `pipeline_weather` under Deployment > Code
Locations, and materialize its assets (`raw_weather` → `weather_table`).

## Demo

<img width="1361" height="674" alt="Screenshot 2026-07-31 001250" src="https://github.com/user-attachments/assets/629fd1e7-df9b-4fb4-a816-8cb7eabb381c" />


## What I'd do differently in production

This uses truncate-and-load (`if_exists="replace"`) instead of incremental
upserts, so historical observations are overwritten on every run instead of
accumulated as a time series — fine for a demo, not for real trend analysis.
There's also no retry/backoff around the Open-Meteo call and no secrets
manager for credentials (they're plain environment variables here). In
production I'd add a `partition` per day so each day's readings are kept,
plus alerting on the asset check failure instead of just a red icon in the UI.
