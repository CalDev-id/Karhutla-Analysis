from datetime import datetime, timezone
from typing import Any

import httpx

from app.services.weather.transform import clean_forecast_days

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 3
TIMEOUT_SECONDS = 15.0
DAILY_FIELDS = ",".join(
    (
        "weather_code",
        "temperature_2m_min",
        "temperature_2m_max",
        "temperature_2m_mean",
        "relative_humidity_2m_mean",
        "precipitation_sum",
        "precipitation_probability_max",
        "wind_speed_10m_max",
        "wind_direction_10m_dominant",
        "uv_index_max",
    )
)


class OpenMeteoForecastError(Exception):
    """Open-Meteo weather forecast cannot be retrieved or parsed."""


async def get_regional_forecast(region: dict[str, Any]) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.get(
                FORECAST_URL,
                params={
                    "latitude": region["latitude"],
                    "longitude": region["longitude"],
                    "daily": DAILY_FIELDS,
                    "forecast_days": FORECAST_DAYS,
                    "timezone": "auto",
                },
            )
            response.raise_for_status()
            payload = response.json()
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        raise OpenMeteoForecastError from exc
    if not isinstance(payload, dict):
        raise OpenMeteoForecastError
    data = clean_forecast_days(payload.get("daily"))
    if not data:
        raise OpenMeteoForecastError
    return {
        "source": "Open-Meteo",
        "region": {"region_id": region["id"], "region_name": region["name"]},
        "issued_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "period": {"start": data[0]["forecast_date"], "end": data[-1]["forecast_date"], "timezone": payload.get("timezone")},
        "data": data,
    }
