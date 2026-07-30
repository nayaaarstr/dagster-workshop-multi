from unittest.mock import Mock, patch

import pytest
import requests

import source


def test_fetch_current_weather_returns_parsed_json():
    fake_response = Mock()
    fake_response.json.return_value = {
        "current_weather": {"temperature": 30.5, "windspeed": 12.0, "time": "2026-07-30T06:00"}
    }
    fake_response.raise_for_status.return_value = None

    with patch("source.requests.get", return_value=fake_response) as mock_get:
        result = source.fetch_current_weather(-6.2088, 106.8456)

    assert result["current_weather"]["temperature"] == 30.5
    mock_get.assert_called_once_with(
        source.BASE_URL,
        params={"latitude": -6.2088, "longitude": 106.8456, "current_weather": "true"},
        timeout=10,
    )


def test_fetch_current_weather_raises_source_unavailable_on_network_error():
    with patch("source.requests.get", side_effect=requests.ConnectionError("boom")):
        with pytest.raises(source.SourceUnavailableError):
            source.fetch_current_weather(-6.2088, 106.8456)
