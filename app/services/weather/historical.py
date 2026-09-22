from asyncio import sleep
from datetime import date
from typing import Any, Optional
from urllib.parse import quote

import httpx

from app.config import environment
from app.services.weather.transform import clean_weather_days

TIMELINE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
TIMEOUT_SECONDS = 10.0


class VisualCrossingError(Exception):
    """Visual Crossing data cannot be retrieved or parsed."""


class VisualCrossingConfigurationError(Exception):
    """The Visual Crossing API key is missing."""


def _api_key() -> str:
    api_key = environment("VISUAL_CROSSING_API_KEY")
    if not api_key:
        raise VisualCrossingConfigurationError
    return api_key


def _number(value: Any) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _wind_direction(value: Any) -> Optional[str]:
    degrees = _number(value)
    if degrees is None:
        return None
    points = ("N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW")
    return points[int((degrees % 360 + 11.25) // 22.5) % len(points)]


async def _fetch_history(location: str, start_date: date, end_date: date) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            params = {
                "unitGroup": "metric",
                "include": "days",
                "elements": "datetime,temp,humidity,precip,windspeed,winddir,conditions",
                "contentType": "json",
                "key": _api_key(),
            }
            for attempt in range(3):
                response = await client.get(
                    f"{TIMELINE_URL}/{quote(location, safe=',')}/{start_date.isoformat()}/{end_date.isoformat()}",
                    params=params,
                )
                if response.status_code != 429 or attempt == 2:
                    break
                await sleep(2 ** attempt)
            response.raise_for_status()
            payload = response.json()
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        raise VisualCrossingError from exc
    if not isinstance(payload, dict):
        raise VisualCrossingError
    return payload


async def get_history(location: str, start_date: date, end_date: date) -> dict[str, Any]:
    payload = await _fetch_history(location, start_date, end_date)
    days = payload.get("days")
    if not isinstance(days, list) or not all(isinstance(day, dict) for day in days):
        raise VisualCrossingError

    source_days = []
    for day in days:
        day_date = day.get("datetime")
        if not isinstance(day_date, str):
            continue
        source_days.append(
            {
                "datetime": day_date,
                "conditions": day.get("conditions"),
                "temp": _number(day.get("temp")),
                "humidity": _number(day.get("humidity")),
                "precip": _number(day.get("precip")),
                "windspeed": _number(day.get("windspeed")),
                "wind_direction": _wind_direction(day.get("winddir")),
            }
        )

    resolved_location = payload.get("resolvedAddress")
    return {
        "source": "Visual Crossing",
        "location": {
            "name": resolved_location if isinstance(resolved_location, str) else location,
            "latitude": _number(payload.get("latitude")),
            "longitude": _number(payload.get("longitude")),
        },
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "timezone": payload.get("timezone") if isinstance(payload.get("timezone"), str) else None,
        },
        "data": clean_weather_days(source_days),
    }
