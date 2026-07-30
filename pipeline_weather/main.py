import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)

import db
import source


@asset
def raw_weather() -> pd.DataFrame:
    """Current weather for each city in source.CITIES, from Open-Meteo."""
    rows = []
    for city, (lat, lon) in source.CITIES.items():
        payload = source.fetch_current_weather(lat, lon)
        current = payload["current_weather"]
        rows.append(
            {
                "city": city,
                "latitude": lat,
                "longitude": lon,
                "temperature_c": current["temperature"],
                "windspeed_kmh": current["windspeed"],
                "observed_at": current["time"],
            }
        )
    return pd.DataFrame(rows)


@asset_check(asset=raw_weather)
def temperature_in_plausible_range(raw_weather: pd.DataFrame) -> AssetCheckResult:
    """Fails if any reading is outside Earth's recorded temperature extremes."""
    bad_rows = raw_weather[
        (raw_weather["temperature_c"] < -90) | (raw_weather["temperature_c"] > 60)
    ]
    return AssetCheckResult(
        passed=bad_rows.empty,
        metadata={"bad_row_count": len(bad_rows)},
    )


@asset
def weather_table(raw_weather: pd.DataFrame) -> int:
    return db.load_table(raw_weather, "weather_observations")


refresh_weather_job = define_asset_job(name="refresh_weather_job")

refresh_weather_daily = ScheduleDefinition(
    name="refresh_weather_daily",
    job=refresh_weather_job,
    cron_schedule="0 6 * * *",
)

defs = Definitions(
    assets=[raw_weather, weather_table],
    asset_checks=[temperature_in_plausible_range],
    jobs=[refresh_weather_job],
    schedules=[refresh_weather_daily],
)
