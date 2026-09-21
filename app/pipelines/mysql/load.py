import argparse
import asyncio
from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

import pymysql

from app.database.mysql.connection import connection
from app.services.air_quality.service import get_air_quality
from app.services.fire.hotspots import get_hotspots
from app.services.regions.service import get_provinces_with_regencies, get_regencies, get_region
from app.services.weather.historical import get_history
from app.services.weather.regional_forecast import get_regional_forecast

DATABASE = "enviromental_conditions"
BATCH_SIZE = 1_000


def _utc_datetime(value: str) -> datetime:
    """Convert an ISO timestamp returned by an upstream API to UTC-naive MySQL DATETIME."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _daily_datetime(value: str) -> datetime:
    return datetime.combine(date.fromisoformat(value), datetime.min.time())


def _local_to_utc_datetime(value: str, timezone_name: Optional[str]) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    if not timezone_name:
        raise ValueError("Timezone Open-Meteo tidak tersedia.")
    return parsed.replace(tzinfo=ZoneInfo(timezone_name)).astimezone(timezone.utc).replace(tzinfo=None)


def _execute_many(
    sql: str, rows: Iterable[tuple[Any, ...]], database_connection: Any = None
) -> int:
    records = list(rows)
    if not records:
        return 0
    if database_connection is not None:
        database_connection.ping(reconnect=True)
        with database_connection.cursor() as cursor:
            affected_rows = 0
            for start in range(0, len(records), BATCH_SIZE):
                cursor.executemany(sql, records[start:start + BATCH_SIZE])
                affected_rows += cursor.rowcount
            return affected_rows
    with connection() as database_connection:
        with database_connection.cursor() as cursor:
            cursor.execute(f"USE `{DATABASE}`")
            affected_rows = 0
            for start in range(0, len(records), BATCH_SIZE):
                cursor.executemany(sql, records[start:start + BATCH_SIZE])
                affected_rows += cursor.rowcount
            return affected_rows


def _load_master(provinces: list[dict[str, Any]]) -> dict[str, int]:
    province_rows = [
        (
            int(province["province_id"]), province["province_name"], province["population"],
            province["total_area_km2"], province["latitude"], province["longitude"], province["timezone"],
        )
        for province in provinces
    ]
    province_sql = """
        INSERT INTO tb_m_province
            (province_id, province_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            province_name = VALUES(province_name), population = VALUES(population),
            total_area_km2 = VALUES(total_area_km2), latitude = VALUES(latitude),
            longitude = VALUES(longitude), timezone = VALUES(timezone)
        """
    regency_rows = [
        (
            regency["regency_id"], int(province["province_id"]), regency["regency_name"],
            regency["population"], regency["total_area_km2"], regency["latitude"],
            regency["longitude"], regency["timezone"],
        )
        for province in provinces
        for regency in province["regencies"]
    ]
    regency_sql = """
        INSERT INTO tb_r_regency_city
            (regency_city_id, province_id, regency_city_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            province_id = VALUES(province_id), regency_city_name = VALUES(regency_city_name),
            population = VALUES(population), total_area_km2 = VALUES(total_area_km2),
            latitude = VALUES(latitude), longitude = VALUES(longitude), timezone = VALUES(timezone)
        """
    with connection() as database_connection:
        with database_connection.cursor() as cursor:
            cursor.execute(f"USE `{DATABASE}`")
        provinces_loaded = _execute_many(province_sql, province_rows, database_connection)
        regencies_loaded = _execute_many(regency_sql, regency_rows, database_connection)
    return {"provinces": provinces_loaded, "regencies": regencies_loaded}


async def load_master_regions() -> dict[str, int]:
    """Extract master regions from Emsifa and upsert them into MySQL."""
    payload = await get_provinces_with_regencies()
    return _load_master(payload["data"])


async def load_region_data(
    region_id: str, start_date: date, end_date: date, hotspot_days: int, database_connection: Any = None
) -> dict[str, int]:
    """Load historical weather, hotspots, and air quality for one regency/city."""
    region = await get_region(region_id)
    if region["level"] != "regency_city":
        raise ValueError("ETL data hanya mendukung region_id kabupaten/kota (NN.NN).")
    province = await get_region(region["parent_id"])

    location = f"{region['latitude']},{region['longitude']}"
    weather, hotspots, air_quality, forecast = await asyncio.gather(
        get_history(location, start_date, end_date),
        get_hotspots(hotspot_days, region, limit=1_000_000),
        get_air_quality(region, hotspot_days),
        get_regional_forecast(region),
    )

    if database_connection is not None:
        return _persist_region_data(region, province, weather, hotspots, air_quality, forecast, database_connection)
    with connection() as database_connection:
        with database_connection.cursor() as cursor:
            cursor.execute(f"USE `{DATABASE}`")
        return _persist_region_data(region, province, weather, hotspots, air_quality, forecast, database_connection)


def _persist_region_data(
    region: dict[str, Any],
    province: dict[str, Any],
    weather: dict[str, Any],
    hotspots: dict[str, Any],
    air_quality: dict[str, Any],
    forecast: dict[str, Any],
    database_connection: Any,
) -> dict[str, int]:
    """Write one region's already-clean source payloads using one MySQL connection."""
    region_id = region["id"]
    _upsert_region_master(database_connection, province, region)

    weather_loaded = _execute_many(
        """
        INSERT INTO tb_r_weather
            (regency_city_id, weather, measured_at, temperature, humidity, precipitation_mm, wind_speed_kmh, wind_direction, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            weather = VALUES(weather), temperature = VALUES(temperature), humidity = VALUES(humidity),
            precipitation_mm = VALUES(precipitation_mm), wind_speed_kmh = VALUES(wind_speed_kmh),
            wind_direction = VALUES(wind_direction)
        """,
        (
            (
                region_id, item["weather"], _daily_datetime(item["time"]), item["temperature_c"],
                item["humidity_pct"], item["precipitation_mm"], item["wind_speed_kmh"],
                item["wind_direction"], weather["source"],
            )
            for item in weather["data"]
        ),
        database_connection,
    )
    hotspots_loaded = _execute_many(
        """
        INSERT INTO tb_r_hotspot
            (regency_city_id, latitude, longitude, detected_at, confidence, frp_mw, daynight, sensor, brightness_k, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            confidence = VALUES(confidence), frp_mw = VALUES(frp_mw), daynight = VALUES(daynight),
            sensor = VALUES(sensor), brightness_k = VALUES(brightness_k)
        """,
        (
            (
                region_id, item["latitude"], item["longitude"], _utc_datetime(item["time"]),
                item["confidence"], item["frp_mw"], item["daynight"], hotspots["sensor"],
                item["brightness_k"], hotspots["source"],
            )
            for item in hotspots["data"]
        ),
        database_connection,
    )
    air_quality_loaded = _execute_many(
        """
        INSERT INTO tb_r_air_quality
            (regency_city_id, measured_at, pm25, pm10, co, no2, aqi, dust_ugm3, aerosol_optical_depth, uv_index, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pm25 = VALUES(pm25), pm10 = VALUES(pm10), co = VALUES(co), no2 = VALUES(no2),
            aqi = VALUES(aqi), dust_ugm3 = VALUES(dust_ugm3),
            aerosol_optical_depth = VALUES(aerosol_optical_depth), uv_index = VALUES(uv_index)
        """,
        (
            (
                region_id, _local_to_utc_datetime(item["time"], air_quality["period"]["timezone"]), item["pm2_5_ugm3"], item["pm10_ugm3"],
                item["carbon_monoxide_ugm3"], item["nitrogen_dioxide_ugm3"], item["us_aqi"],
                item["dust_ugm3"], item["aerosol_optical_depth"], item["uv_index"], air_quality["source"],
            )
            for item in air_quality["data"]
        ),
        database_connection,
    )
    forecast_loaded = _execute_many(
        """
        INSERT INTO tb_r_weather_forecast
            (regency_city_id, forecast_date, issued_at, weather, weather_code, temperature_min_c,
             temperature_max_c, temperature_avg_c, humidity_avg_pct, precipitation_mm,
             precipitation_probability_max_pct, wind_speed_max_kmh, wind_direction_deg,
             wind_direction, uv_index_max, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            issued_at = VALUES(issued_at), weather = VALUES(weather), weather_code = VALUES(weather_code),
            temperature_min_c = VALUES(temperature_min_c), temperature_max_c = VALUES(temperature_max_c),
            temperature_avg_c = VALUES(temperature_avg_c), humidity_avg_pct = VALUES(humidity_avg_pct),
            precipitation_mm = VALUES(precipitation_mm),
            precipitation_probability_max_pct = VALUES(precipitation_probability_max_pct),
            wind_speed_max_kmh = VALUES(wind_speed_max_kmh), wind_direction_deg = VALUES(wind_direction_deg),
            wind_direction = VALUES(wind_direction), uv_index_max = VALUES(uv_index_max)
        """,
        (
            (
                region_id, item["forecast_date"], forecast["issued_at"], item["weather"], item["weather_code"],
                item["temperature_min_c"], item["temperature_max_c"], item["temperature_avg_c"],
                item["humidity_avg_pct"], item["precipitation_mm"],
                item["precipitation_probability_max_pct"], item["wind_speed_max_kmh"],
                item["wind_direction_deg"], item["wind_direction"], item["uv_index_max"], forecast["source"],
            )
            for item in forecast["data"]
        ),
        database_connection,
    )
    return {
        "weather": weather_loaded,
        "hotspots": hotspots_loaded,
        "air_quality": air_quality_loaded,
        "forecast": forecast_loaded,
    }


def _upsert_region_master(
    database_connection: Any, province: dict[str, Any], region: dict[str, Any]
) -> None:
    _execute_many("""
        INSERT INTO tb_m_province
            (province_id, province_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            province_name = VALUES(province_name), population = VALUES(population),
            total_area_km2 = VALUES(total_area_km2), latitude = VALUES(latitude),
            longitude = VALUES(longitude), timezone = VALUES(timezone)
    """, [(int(province["id"]), province["name"], province["population"], province["total_area_km2"],
            province["latitude"], province["longitude"], province["timezone"])], database_connection)
    _execute_many("""
        INSERT INTO tb_r_regency_city
            (regency_city_id, province_id, regency_city_name, population, total_area_km2, latitude, longitude, timezone)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            province_id = VALUES(province_id), regency_city_name = VALUES(regency_city_name),
            population = VALUES(population), total_area_km2 = VALUES(total_area_km2),
            latitude = VALUES(latitude), longitude = VALUES(longitude), timezone = VALUES(timezone)
    """, [(region["id"], int(province["id"]), region["name"], region["population"],
            region["total_area_km2"], region["latitude"], region["longitude"], region["timezone"])], database_connection)


async def load_province_data(
    province_id: str, start_date: date, end_date: date, hotspot_days: int
) -> dict[str, Any]:
    """Load data for every regency/city in one province with limited concurrency."""
    regencies = (await get_regencies(province_id))["data"]
    # Visual Crossing may reject bursts from one API key; process one region at a time.
    semaphore = asyncio.Semaphore(1)

    async def load_one(regency: dict[str, str]) -> dict[str, Any]:
        async with semaphore:
            loaded = await load_region_data(regency["id"], start_date, end_date, hotspot_days)
            return {"region_id": regency["id"], "region_name": regency["name"], "loaded": loaded}

    data = await asyncio.gather(*(load_one(regency) for regency in regencies))
    totals = {
        "weather": sum(item["loaded"]["weather"] for item in data),
        "hotspots": sum(item["loaded"]["hotspots"] for item in data),
        "air_quality": sum(item["loaded"]["air_quality"] for item in data),
        "forecast": sum(item["loaded"]["forecast"] for item in data),
    }
    return {"total_regencies": len(data), "totals": totals, "data": data}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load clean Karhutla data into MySQL.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("master", help="Load provinces and regencies from Emsifa.")
    region = commands.add_parser("region", help="Load data for one regency/city.")
    region.add_argument("region_id", help="Emsifa regency/city ID, e.g. 61.01")
    region.add_argument("--start-date", type=date.fromisoformat, required=True)
    region.add_argument("--end-date", type=date.fromisoformat, required=True)
    region.add_argument("--hotspot-days", type=int, default=1, choices=range(1, 6))
    return parser.parse_args()


async def _main() -> None:
    arguments = _arguments()
    if arguments.command == "master":
        print(await load_master_regions())
        return
    if arguments.end_date < arguments.start_date:
        raise ValueError("end-date harus sama atau setelah start-date.")
    if (arguments.end_date - arguments.start_date).days + 1 > 30:
        raise ValueError("Rentang weather maksimal 30 hari.")
    print(await load_region_data(
        arguments.region_id, arguments.start_date, arguments.end_date, arguments.hotspot_days
    ))


if __name__ == "__main__":
    asyncio.run(_main())
