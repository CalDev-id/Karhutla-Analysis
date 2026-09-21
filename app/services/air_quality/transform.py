from typing import Any, Optional


def _number(value: Any) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def clean_hourly_air_quality(hourly: Any) -> list[dict[str, Any]]:
    if not isinstance(hourly, dict):
        return []
    times = hourly.get("time")
    if not isinstance(times, list):
        return []

    fields = {
        "us_aqi": "us_aqi",
        "pm2_5": "pm2_5_ugm3",
        "pm10": "pm10_ugm3",
        "nitrogen_dioxide": "nitrogen_dioxide_ugm3",
        "carbon_monoxide": "carbon_monoxide_ugm3",
        "dust": "dust_ugm3",
        "aerosol_optical_depth": "aerosol_optical_depth",
        "uv_index": "uv_index",
    }
    data = []
    for index, observed_at in enumerate(times):
        if not isinstance(observed_at, str):
            continue
        record = {"time": observed_at}
        for source_field, target_field in fields.items():
            values = hourly.get(source_field)
            value = values[index] if isinstance(values, list) and index < len(values) else None
            record[target_field] = _number(value)
        data.append(record)
    return data
