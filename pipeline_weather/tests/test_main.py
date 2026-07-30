from unittest.mock import patch

import pandas as pd
from dagster import materialize

import db
import source
from main import raw_weather, temperature_in_plausible_range, weather_table

FAKE_PAYLOAD = {
    "current_weather": {"temperature": 28.0, "windspeed": 10.0, "time": "2026-07-30T06:00"}
}


def test_weather_pipeline_loads_expected_rows():
    loaded = {}

    def fake_load_table(df: pd.DataFrame, table_name: str) -> int:
        loaded[table_name] = df
        return len(df)

    with patch.object(
        source, "fetch_current_weather", return_value=FAKE_PAYLOAD
    ), patch.object(db, "load_table", side_effect=fake_load_table):
        result = materialize(
            [raw_weather, weather_table], asset_checks=[temperature_in_plausible_range]
        )

    assert result.success
    table = loaded["weather_observations"]
    assert set(table["city"]) == set(source.CITIES.keys())
    assert (table["temperature_c"] == 28.0).all()


def test_temperature_check_fails_on_implausible_reading():
    bad_payload = {
        "current_weather": {"temperature": 999.0, "windspeed": 10.0, "time": "2026-07-30T06:00"}
    }

    with patch.object(source, "fetch_current_weather", return_value=bad_payload), patch.object(
        db, "load_table", return_value=0
    ):
        result = materialize(
            [raw_weather, weather_table], asset_checks=[temperature_in_plausible_range]
        )

    check_result = next(
        e for e in result.get_asset_check_evaluations()
        if e.check_name == "temperature_in_plausible_range"
    )
    assert check_result.passed is False
