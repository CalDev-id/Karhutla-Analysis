from datetime import datetime
from typing import Any, Optional


def _number(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _timestamp(row: dict[str, str]) -> Optional[str]:
    date_value = row.get("acq_date")
    time_value = (row.get("acq_time") or "").zfill(4)
    if not date_value or not time_value.isdigit():
        return None
    try:
        observed_at = datetime.strptime(f"{date_value} {time_value}", "%Y-%m-%d %H%M")
    except ValueError:
        return None
    return observed_at.strftime("%Y-%m-%dT%H:%M:00Z")


def _point_in_ring(longitude: float, latitude: float, ring: list[list[float]]) -> bool:
    inside = False
    previous = len(ring) - 1
    for current, point in enumerate(ring):
        current_longitude, current_latitude = point
        previous_longitude, previous_latitude = ring[previous]
        if (current_latitude > latitude) != (previous_latitude > latitude):
            intersection = (previous_longitude - current_longitude) * (latitude - current_latitude)
            intersection /= previous_latitude - current_latitude
            if longitude < intersection + current_longitude:
                inside = not inside
        previous = current
    return inside


def bounds(paths: list[list[list[float]]]) -> tuple[float, float, float, float]:
    points = [point for ring in paths for point in ring]
    longitudes = [point[0] for point in points]
    latitudes = [point[1] for point in points]
    return min(longitudes), min(latitudes), max(longitudes), max(latitudes)


def clean_hotspots(
    rows: list[dict[str, str]], sensor: str, region: dict[str, Any], paths: list[list[list[float]]]
) -> list[dict[str, Any]]:
    data = []
    seen = set()
    for row in rows:
        latitude = _number(row.get("latitude"))
        longitude = _number(row.get("longitude"))
        observed_at = _timestamp(row)
        frp_mw = _number(row.get("frp"))
        brightness_k = _number(row.get("bright_ti4"))
        if (
            latitude is None or longitude is None or observed_at is None or frp_mw is None or frp_mw < 0
            or not -90 <= latitude <= 90 or not -180 <= longitude <= 180
            or not any(_point_in_ring(longitude, latitude, ring) for ring in paths)
        ):
            continue
        fingerprint = (sensor, observed_at, latitude, longitude)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        data.append({
            "region_id": region["id"], "region_name": region["name"],
            "latitude": latitude, "longitude": longitude, "time": observed_at,
            "confidence": row.get("confidence") or None, "frp_mw": frp_mw,
            "brightness_k": brightness_k if brightness_k is None or brightness_k >= 0 else None,
            "daynight": row.get("daynight") or None,
        })
    return data


def summarize_hotspots(data: list[dict[str, Any]]) -> dict[str, Any]:
    frp_values = [hotspot["frp_mw"] for hotspot in data]
    return {
        "count": len(data),
        "average_frp_mw": sum(frp_values) / len(frp_values) if frp_values else None,
        "max_frp_mw": max(frp_values) if frp_values else None,
        "latest_time": max((hotspot["time"] for hotspot in data), default=None),
    }
