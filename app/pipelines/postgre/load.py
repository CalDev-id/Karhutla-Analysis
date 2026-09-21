import asyncio
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from psycopg2.extras import execute_batch

from app.database.postgre.connection import connection
from app.services.air_quality.service import get_air_quality
from app.services.fire.hotspots import get_hotspots
from app.services.regions.service import get_provinces_with_regencies, get_regencies, get_region
from app.services.weather.historical import get_history
from app.services.weather.regional_forecast import get_regional_forecast


def _utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def _local_to_utc_datetime(value: str, timezone_name: Optional[str]) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if not timezone_name:
        raise ValueError("Timezone Open-Meteo tidak tersedia.")
    return parsed.replace(tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc).replace(tzinfo=None)


def _execute_many(database_connection: Any, sql: str, rows: Iterable[tuple[Any, ...]]) -> int:
    records = list(rows)
    if not records:
        return 0
    with database_connection.cursor() as cursor:
        execute_batch(cursor, sql, records, page_size=1_000)
    return len(records)


async def load_master_regions() -> dict[str, int]:
    provinces = (await get_provinces_with_regencies())["data"]
    province_rows = [
        (int(item["province_id"]), item["province_name"], item["population"], item["total_area_km2"],
         item["latitude"], item["longitude"], item["timezone"])
        for item in provinces
    ]
    regency_rows = [
        (item["regency_id"], int(province["province_id"]), item["regency_name"], item["population"],
         item["total_area_km2"], item["latitude"], item["longitude"], item["timezone"])
        for province in provinces for item in province["regencies"]
    ]
    with connection() as database_connection:
        provinces_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_m_province
                (province_id, province_name, population, total_area_km2, latitude, longitude, timezone)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (province_id) DO UPDATE SET
                province_name = EXCLUDED.province_name, population = EXCLUDED.population,
                total_area_km2 = EXCLUDED.total_area_km2, latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude, timezone = EXCLUDED.timezone
        """, province_rows)
        regencies_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_r_regency_city
                (regency_city_id, province_id, regency_city_name, population, total_area_km2, latitude, longitude, timezone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regency_city_id) DO UPDATE SET
                province_id = EXCLUDED.province_id, regency_city_name = EXCLUDED.regency_city_name,
                population = EXCLUDED.population, total_area_km2 = EXCLUDED.total_area_km2,
                latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, timezone = EXCLUDED.timezone
        """, regency_rows)
    return {"provinces": provinces_loaded, "regencies": regencies_loaded}


async def load_region_data(region_id: str, start_date: date, end_date: date, days: int) -> dict[str, int]:
    region = await get_region(region_id)
    if region["level"] != "regency_city":
        raise ValueError("ETL data hanya mendukung region_id kabupaten/kota (NN.NN).")
    province = await get_region(region["parent_id"])
    location = f"{region['latitude']},{region['longitude']}"
    weather, hotspots, air_quality, forecast = await asyncio.gather(
        get_history(location, start_date, end_date), get_hotspots(days, region, limit=1_000_000),
        get_air_quality(region, days), get_regional_forecast(region),
    )
    with connection() as database_connection:
        _upsert_region_master(database_connection, province, region)
        weather_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_r_weather
                (regency_city_id, weather, measured_at, temperature, humidity, precipitation_mm, wind_speed_kmh, wind_direction, source_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regency_city_id, measured_at, source_name) DO UPDATE SET
                weather = EXCLUDED.weather, temperature = EXCLUDED.temperature, humidity = EXCLUDED.humidity,
                precipitation_mm = EXCLUDED.precipitation_mm, wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                wind_direction = EXCLUDED.wind_direction
        """, ((region_id, item["weather"], datetime.combine(date.fromisoformat(item["time"]), datetime.min.time()),
                 item["temperature_c"], item["humidity_pct"], item["precipitation_mm"], item["wind_speed_kmh"],
                 item["wind_direction"], weather["source"]) for item in weather["data"]))
        hotspots_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_r_hotspot
                (regency_city_id, latitude, longitude, detected_at, confidence, frp_mw, daynight, sensor, brightness_k, source_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regency_city_id, detected_at, latitude, longitude, source_name) DO UPDATE SET
                confidence = EXCLUDED.confidence, frp_mw = EXCLUDED.frp_mw, daynight = EXCLUDED.daynight,
                sensor = EXCLUDED.sensor, brightness_k = EXCLUDED.brightness_k
        """, ((region_id, item["latitude"], item["longitude"], _utc_datetime(item["time"]), item["confidence"],
                 item["frp_mw"], item["daynight"], hotspots["sensor"], item["brightness_k"], hotspots["source"])
                for item in hotspots["data"]))
        air_quality_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_r_air_quality
                (regency_city_id, measured_at, pm25, pm10, co, no2, aqi, dust_ugm3, aerosol_optical_depth, uv_index, source_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regency_city_id, measured_at, source_name) DO UPDATE SET
                pm25 = EXCLUDED.pm25, pm10 = EXCLUDED.pm10, co = EXCLUDED.co, no2 = EXCLUDED.no2,
                aqi = EXCLUDED.aqi, dust_ugm3 = EXCLUDED.dust_ugm3,
                aerosol_optical_depth = EXCLUDED.aerosol_optical_depth, uv_index = EXCLUDED.uv_index
        """, ((region_id, _local_to_utc_datetime(item["time"], air_quality["period"]["timezone"]), item["pm2_5_ugm3"],
                 item["pm10_ugm3"], item["carbon_monoxide_ugm3"], item["nitrogen_dioxide_ugm3"], item["us_aqi"],
                 item["dust_ugm3"], item["aerosol_optical_depth"], item["uv_index"], air_quality["source"])
                for item in air_quality["data"]))
        forecast_loaded = _execute_many(database_connection, """
            INSERT INTO public.tb_r_weather_forecast
                (regency_city_id, forecast_date, issued_at, weather, weather_code, temperature_min_c, temperature_max_c,
                 temperature_avg_c, humidity_avg_pct, precipitation_mm, precipitation_probability_max_pct,
                 wind_speed_max_kmh, wind_direction_deg, wind_direction, uv_index_max, source_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regency_city_id, forecast_date, source_name) DO UPDATE SET
                issued_at = EXCLUDED.issued_at, weather = EXCLUDED.weather, weather_code = EXCLUDED.weather_code,
                temperature_min_c = EXCLUDED.temperature_min_c, temperature_max_c = EXCLUDED.temperature_max_c,
                temperature_avg_c = EXCLUDED.temperature_avg_c, humidity_avg_pct = EXCLUDED.humidity_avg_pct,
                precipitation_mm = EXCLUDED.precipitation_mm,
                precipitation_probability_max_pct = EXCLUDED.precipitation_probability_max_pct,
                wind_speed_max_kmh = EXCLUDED.wind_speed_max_kmh, wind_direction_deg = EXCLUDED.wind_direction_deg,
                wind_direction = EXCLUDED.wind_direction, uv_index_max = EXCLUDED.uv_index_max
        """, ((region_id, item["forecast_date"], forecast["issued_at"], item["weather"], item["weather_code"],
                 item["temperature_min_c"], item["temperature_max_c"], item["temperature_avg_c"],
                 item["humidity_avg_pct"], item["precipitation_mm"], item["precipitation_probability_max_pct"],
                 item["wind_speed_max_kmh"], item["wind_direction_deg"], item["wind_direction"], item["uv_index_max"],
                 forecast["source"]) for item in forecast["data"]))
    return {"weather": weather_loaded, "hotspots": hotspots_loaded, "air_quality": air_quality_loaded, "forecast": forecast_loaded}


def _upsert_region_master(database_connection: Any, province: dict[str, Any], region: dict[str, Any]) -> None:
    _execute_many(database_connection, """
        INSERT INTO public.tb_m_province
            (province_id, province_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (province_id) DO UPDATE SET
            province_name = EXCLUDED.province_name, population = EXCLUDED.population,
            total_area_km2 = EXCLUDED.total_area_km2, latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude, timezone = EXCLUDED.timezone
    """, [(int(province["id"]), province["name"], province["population"], province["total_area_km2"],
            province["latitude"], province["longitude"], province["timezone"])])
    _execute_many(database_connection, """
        INSERT INTO public.tb_r_regency_city
            (regency_city_id, province_id, regency_city_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (regency_city_id) DO UPDATE SET
            province_id = EXCLUDED.province_id, regency_city_name = EXCLUDED.regency_city_name,
            population = EXCLUDED.population, total_area_km2 = EXCLUDED.total_area_km2,
            latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, timezone = EXCLUDED.timezone
    """, [(region["id"], int(province["id"]), region["name"], region["population"],
            region["total_area_km2"], region["latitude"], region["longitude"], region["timezone"])])


async def load_province_data(province_id: str, start_date: date, end_date: date, days: int) -> dict[str, Any]:
    regencies = (await get_regencies(province_id))["data"]
    data = []
    for regency in regencies:
        loaded = await load_region_data(regency["id"], start_date, end_date, days)
        data.append({"region_id": regency["id"], "region_name": regency["name"], "loaded": loaded})
    return {
        "total_regencies": len(data),
        "totals": {field: sum(item["loaded"][field] for item in data) for field in ("weather", "hotspots", "air_quality", "forecast")},
        "data": data,
    }
