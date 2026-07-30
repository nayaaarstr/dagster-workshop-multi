import requests

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# City -> (latitude, longitude). Add more cities here if you want.
CITIES = {
    "Jakarta": (-6.2088, 106.8456),
    "Bandung": (-6.9175, 107.6191),
    "Surabaya": (-7.2575, 112.7521),
    "Yogyakarta": (-7.7956, 110.3695),
    "Medan": (3.5952, 98.6722),
}


class SourceUnavailableError(Exception):
    """Raised when api.open-meteo.com cannot be reached."""


def fetch_current_weather(lat: float, lon: float) -> dict:
    """Fetch current weather for a single lat/lon from Open-Meteo."""
    try:
        response = requests.get(
            BASE_URL,
            params={"latitude": lat, "longitude": lon, "current_weather": "true"},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SourceUnavailableError(
            "Could not reach api.open-meteo.com — check your internet connection"
        ) from exc
    return response.json()
