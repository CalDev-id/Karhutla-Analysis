from datetime import date
from typing import Any, Optional


WMO_WEATHER = {
    0: "Cerah",
    1: "Sebagian cerah",
    2: "Berawan",
    3: "Mendung",
    45: "Kabut",
    48: "Kabut beku",
    51: "Gerimis ringan",
    53: "Gerimis sedang",
    55: "Gerimis lebat",
    61: "Hujan ringan",
    63: "Hujan sedang",
    65: "Hujan lebat",
    80: "Hujan lokal ringan",
    81: "Hujan lokal sedang",
    82: "Hujan lokal lebat",
    95: "Badai petir",
    96: "Badai petir dengan hujan es ringan",
    99: "Badai petir dengan hujan es lebat",
}


def _valid_number(value: Any, minimum: Optional[float] = None, maximum: Optional[float] = None) -> Optional[float]:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    number = float(value)
    if (minimum is not None and number < minimum) or (maximum is not None and number > maximum):
        return None
    return number


def clean_weather_days(days: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = []
    for day in days:
        day_date = day.get("datetime")
        if not isinstance(day_date, str):
            continue
        try:
            date.fromisoformat(day_date)
        except ValueError:
            continue

        temperature_c = _valid_number(day.get("temp"), -90, 60)
        humidity_pct = _valid_number(day.get("humidity"), 0, 100)
        precipitation_mm = _valid_number(day.get("precip"), 0)
        wind_speed_kmh = _valid_number(day.get("windspeed"), 0)
        if any(value is None for value in (temperature_c, humidity_pct, precipitation_mm, wind_speed_kmh)):
            continue
        data.append({
            "time": day_date,
            "weather": day.get("conditions") if isinstance(day.get("conditions"), str) else None,
            "temperature_c": temperature_c,
            "humidity_pct": humidity_pct,
            "precipitation_mm": precipitation_mm,
            "wind_speed_kmh": wind_speed_kmh,
            "wind_direction": day.get("wind_direction"),
        })
    return data


def clean_forecast_days(daily: Any) -> list[dict[str, Any]]:
    if not isinstance(daily, dict) or not isinstance(daily.get("time"), list):
        return []

    fields = {
        "weather_code": ("weather_code", 0, 99),
        "temperature_min_c": ("temperature_2m_min", -90, 60),
        "temperature_max_c": ("temperature_2m_max", -90, 60),
        "temperature_avg_c": ("temperature_2m_mean", -90, 60),
        "humidity_avg_pct": ("relative_humidity_2m_mean", 0, 100),
        "precipitation_mm": ("precipitation_sum", 0, None),
        "precipitation_probability_max_pct": ("precipitation_probability_max", 0, 100),
        "wind_speed_max_kmh": ("wind_speed_10m_max", 0, None),
        "wind_direction_deg": ("wind_direction_10m_dominant", 0, 360),
        "uv_index_max": ("uv_index_max", 0, None),
    }
    data = []
    for index, forecast_date in enumerate(daily["time"]):
        if not isinstance(forecast_date, str):
            continue
        try:
            date.fromisoformat(forecast_date)
        except ValueError:
            continue
        record: dict[str, Any] = {"forecast_date": forecast_date}
        for target, (source, minimum, maximum) in fields.items():
            values = daily.get(source)
            value = values[index] if isinstance(values, list) and index < len(values) else None
            record[target] = _valid_number(value, minimum, maximum)
        weather_code = record["weather_code"]
        record["weather_code"] = int(weather_code) if weather_code is not None else None
        record["weather"] = WMO_WEATHER.get(record["weather_code"])
        wind_degrees = record["wind_direction_deg"]
        record["wind_direction"] = (
            ("N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW")
            [int((wind_degrees + 11.25) // 22.5) % 16]
            if wind_degrees is not None else None
        )
        data.append(record)
    return data
