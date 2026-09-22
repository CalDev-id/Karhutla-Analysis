from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

BATCH_SIZE = 1_000


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def _air_quality_time(value: str, timezone_name: Optional[str]) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if not timezone_name:
        raise ValueError("Timezone Open-Meteo tidak tersedia.")
    return parsed.replace(tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc).replace(tzinfo=None)


def _upsert(connection: Any, sql: str, rows: Iterable[tuple[Any, ...]]) -> int:
    records = list(rows)
    if not records:
        return 0
    connection.ping(reconnect=True)
    affected = 0
    with connection.cursor() as cursor:
        for start in range(0, len(records), BATCH_SIZE):
            cursor.executemany(sql, records[start:start + BATCH_SIZE])
            affected += cursor.rowcount
    return affected


def save_region_data(
    connection: Any, province: dict[str, Any], region: dict[str, Any], weather: dict[str, Any],
    hotspots: dict[str, Any], air_quality: dict[str, Any], forecast: dict[str, Any],
) -> dict[str, int]:
    region_id = region["id"]
    _upsert(connection, """INSERT INTO tb_m_province (province_id, province_name, population, total_area_km2, latitude, longitude, timezone) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE province_name=VALUES(province_name), population=VALUES(population), total_area_km2=VALUES(total_area_km2), latitude=VALUES(latitude), longitude=VALUES(longitude), timezone=VALUES(timezone)""", [(int(province["id"]), province["name"], province["population"], province["total_area_km2"], province["latitude"], province["longitude"], province["timezone"])])
    _upsert(connection, """INSERT INTO tb_r_regency_city (regency_city_id, province_id, regency_city_name, population, total_area_km2, latitude, longitude, timezone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE province_id=VALUES(province_id), regency_city_name=VALUES(regency_city_name), population=VALUES(population), total_area_km2=VALUES(total_area_km2), latitude=VALUES(latitude), longitude=VALUES(longitude), timezone=VALUES(timezone)""", [(region_id, int(province["id"]), region["name"], region["population"], region["total_area_km2"], region["latitude"], region["longitude"], region["timezone"])])
    weather_count = _upsert(connection, """INSERT INTO tb_r_weather (regency_city_id, weather, measured_at, temperature, humidity, precipitation_mm, wind_speed_kmh, wind_direction, source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE weather=VALUES(weather), temperature=VALUES(temperature), humidity=VALUES(humidity), precipitation_mm=VALUES(precipitation_mm), wind_speed_kmh=VALUES(wind_speed_kmh), wind_direction=VALUES(wind_direction)""", ((region_id, item["weather"], datetime.combine(date.fromisoformat(item["time"]), datetime.min.time()), item["temperature_c"], item["humidity_pct"], item["precipitation_mm"], item["wind_speed_kmh"], item["wind_direction"], weather["source"]) for item in weather["data"]))
    hotspot_count = _upsert(connection, """INSERT INTO tb_r_hotspot (regency_city_id, latitude, longitude, detected_at, confidence, frp_mw, daynight, sensor, brightness_k, source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE confidence=VALUES(confidence), frp_mw=VALUES(frp_mw), daynight=VALUES(daynight), sensor=VALUES(sensor), brightness_k=VALUES(brightness_k)""", ((region_id, item["latitude"], item["longitude"], _utc(item["time"]), item["confidence"], item["frp_mw"], item["daynight"], hotspots["sensor"], item["brightness_k"], hotspots["source"]) for item in hotspots["data"]))
    air_quality_count = _upsert(connection, """INSERT INTO tb_r_air_quality (regency_city_id, measured_at, pm25, pm10, co, no2, aqi, dust_ugm3, aerosol_optical_depth, uv_index, source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE pm25=VALUES(pm25), pm10=VALUES(pm10), co=VALUES(co), no2=VALUES(no2), aqi=VALUES(aqi), dust_ugm3=VALUES(dust_ugm3), aerosol_optical_depth=VALUES(aerosol_optical_depth), uv_index=VALUES(uv_index)""", ((region_id, _air_quality_time(item["time"], air_quality["period"]["timezone"]), item["pm2_5_ugm3"], item["pm10_ugm3"], item["carbon_monoxide_ugm3"], item["nitrogen_dioxide_ugm3"], item["us_aqi"], item["dust_ugm3"], item["aerosol_optical_depth"], item["uv_index"], air_quality["source"]) for item in air_quality["data"]))
    forecast_count = _upsert(connection, """INSERT INTO tb_r_weather_forecast (regency_city_id, forecast_date, issued_at, weather, weather_code, temperature_min_c, temperature_max_c, temperature_avg_c, humidity_avg_pct, precipitation_mm, precipitation_probability_max_pct, wind_speed_max_kmh, wind_direction_deg, wind_direction, uv_index_max, source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE issued_at=VALUES(issued_at), weather=VALUES(weather), weather_code=VALUES(weather_code), temperature_min_c=VALUES(temperature_min_c), temperature_max_c=VALUES(temperature_max_c), temperature_avg_c=VALUES(temperature_avg_c), humidity_avg_pct=VALUES(humidity_avg_pct), precipitation_mm=VALUES(precipitation_mm), precipitation_probability_max_pct=VALUES(precipitation_probability_max_pct), wind_speed_max_kmh=VALUES(wind_speed_max_kmh), wind_direction_deg=VALUES(wind_direction_deg), wind_direction=VALUES(wind_direction), uv_index_max=VALUES(uv_index_max)""", ((region_id, item["forecast_date"], forecast["issued_at"], item["weather"], item["weather_code"], item["temperature_min_c"], item["temperature_max_c"], item["temperature_avg_c"], item["humidity_avg_pct"], item["precipitation_mm"], item["precipitation_probability_max_pct"], item["wind_speed_max_kmh"], item["wind_direction_deg"], item["wind_direction"], item["uv_index_max"], forecast["source"]) for item in forecast["data"]))
    return {"weather": weather_count, "hotspots": hotspot_count, "air_quality": air_quality_count, "forecast": forecast_count}
