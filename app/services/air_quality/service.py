from typing import Any

import httpx

from app.services.air_quality.transform import clean_hourly_air_quality

AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
TIMEOUT_SECONDS = 15.0
HOURLY_FIELDS = ",".join(
    (
        "us_aqi",
        "pm2_5",
        "pm10",
        "nitrogen_dioxide",
        "carbon_monoxide",
        "dust",
        "aerosol_optical_depth",
        "uv_index",
    )
)


class OpenMeteoError(Exception):
    """Open-Meteo air quality data cannot be retrieved or parsed."""


async def get_air_quality(region: dict[str, Any], days: int) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.get(
                AIR_QUALITY_URL,
                params={
                    "latitude": region["latitude"],
                    "longitude": region["longitude"],
                    "hourly": HOURLY_FIELDS,
                    "past_days": days - 1,
                    "forecast_days": 1,
                    "timezone": "auto",
                },
            )
            response.raise_for_status()
            payload = response.json()
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        raise OpenMeteoError from exc

    if not isinstance(payload, dict):
        raise OpenMeteoError
    data = clean_hourly_air_quality(payload.get("hourly"))
    if not data:
        raise OpenMeteoError
    return {
        "source": "Open-Meteo",
        "region": {
            "region_id": region["id"],
            "region_name": region["name"],
            "latitude": region["latitude"],
            "longitude": region["longitude"],
        },
        "period": {
            "days": days,
            "timezone": payload.get("timezone") if isinstance(payload.get("timezone"), str) else None,
        },
        "granularity": "hourly",
        "data": data,
    }
