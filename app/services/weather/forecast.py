from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone, tzinfo
from math import atan2, cos, degrees, radians, sin
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

FORECAST_URL = "https://api.bmkg.go.id/publik/prakiraan-cuaca"
TIMEOUT_SECONDS = 10.0


class BMKGError(Exception):
    """BMKG forecast data cannot be retrieved or parsed."""


async def _fetch_forecast(adm4: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.get(FORECAST_URL, params={"adm4": adm4})
            response.raise_for_status()
            payload = response.json()
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        raise BMKGError from exc
    if not isinstance(payload, dict):
        raise BMKGError
    return payload


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise BMKGError
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BMKGError from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def _location_timezone(value: Any) -> tzinfo:
    if not isinstance(value, str):
        return timezone.utc
    try:
        if "/" in value:
            return ZoneInfo(value)
        sign = 1 if value.startswith("+") else -1
        return timezone(sign * timedelta(hours=int(value[1:3]), minutes=int(value[3:5])))
    except (IndexError, ValueError, ZoneInfoNotFoundError):
        return timezone.utc


def _number(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None and not isinstance(value, bool) else None
    except (TypeError, ValueError):
        return None


def _mean(values: list[float]) -> Optional[float]:
    return round(sum(values) / len(values), 1) if values else None


def _wind_direction(values: list[float]) -> Optional[str]:
    if not values:
        return None
    sin_total = sum(sin(radians(value)) for value in values)
    cos_total = sum(cos(radians(value)) for value in values)
    if abs(sin_total) < 1e-9 and abs(cos_total) < 1e-9:
        return None
    angle = degrees(atan2(sin_total, cos_total)) % 360
    points = ("N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW")
    return points[int((angle + 11.25) // 22.5) % len(points)]


def _wind_degrees(forecast: dict[str, Any]) -> Optional[float]:
    degree_value = _number(forecast.get("wd_deg"))
    if degree_value is not None:
        return degree_value
    compass = forecast.get("wd")
    directions = {"N": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5, "E": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5, "S": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5, "W": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5}
    return directions.get(compass) if isinstance(compass, str) else None


async def get_forecast(adm4: str) -> dict[str, Any]:
    payload = await _fetch_forecast(adm4)
    try:
        location = payload["lokasi"]
        groups = payload["data"][0]["cuaca"]
    except (KeyError, IndexError, TypeError) as exc:
        raise BMKGError from exc
    forecasts = [item for group in groups if isinstance(group, list) for item in group]
    if not isinstance(location, dict) or not forecasts or not all(isinstance(item, dict) for item in forecasts):
        raise BMKGError

    local_timezone = _location_timezone(location.get("timezone"))
    future: list[tuple[datetime, dict[str, Any]]] = []
    for forecast in forecasts:
        try:
            forecast_time = _parse_time(forecast.get("datetime") or forecast.get("utc_datetime"))
        except BMKGError:
            continue
        if forecast_time >= datetime.now(timezone.utc):
            future.append((forecast_time, forecast))
    if not future:
        raise BMKGError

    location_name = location.get("desa") or location.get("kecamatan") or location.get("kotkab")
    if not isinstance(location_name, str) or not location_name:
        raise BMKGError

    daily: dict[str, list[tuple[datetime, dict[str, Any]]]] = defaultdict(list)
    for forecast_time, forecast in future:
        local_time = forecast_time.astimezone(local_timezone)
        daily[local_time.date().isoformat()].append((forecast_time, forecast))

    data = []
    for day in sorted(daily)[:3]:
        items = daily[day]
        temperatures = [_number(item.get("t")) for _, item in items]
        humidities = [_number(item.get("hu")) for _, item in items]
        precipitation = [_number(item.get("tp")) for _, item in items]
        wind_speeds = [_number(item.get("ws")) for _, item in items]
        wind_degrees = [_wind_degrees(item) for _, item in items]
        weather_items = [
            (forecast_time, item["weather_desc"])
            for forecast_time, item in items
            if isinstance(item.get("weather_desc"), str)
        ]
        weather = None
        if weather_items:
            counts = Counter(description for _, description in weather_items)
            highest = max(counts.values())
            candidates = {description for description, count in counts.items() if count == highest}
            weather = max((item for item in weather_items if item[1] in candidates), key=lambda item: item[0])[1]
        data.append(
            {
                "time": day,
                "weather": weather,
                "temperature_c": _mean([value for value in temperatures if value is not None]),
                "humidity_pct": _mean([value for value in humidities if value is not None]),
                "precipitation_mm": round(sum(value for value in precipitation if value is not None), 1),
                "wind_speed_kmh": _mean([value for value in wind_speeds if value is not None]),
                "wind_direction": _wind_direction([value for value in wind_degrees if value is not None]),
            }
        )

    return {
        "source": "BMKG",
        "location": {
            "name": location_name,
            "latitude": _number(location.get("lat")),
            "longitude": _number(location.get("lon")),
        },
        "period": {
            "start": data[0]["time"],
            "end": data[-1]["time"],
            "timezone": location.get("timezone") if isinstance(location.get("timezone"), str) else None,
        },
        "granularity": "daily",
        "data": data,
    }
