from datetime import date, datetime, timezone
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from psycopg2.extras import execute_batch


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
    with connection.cursor() as cursor:
        execute_batch(cursor, sql, records, page_size=1_000)
    return len(records)


def create_etl_run(
    connection: Any,
    pipeline_name: str,
    dag_run_id: str,
    task_id: str,
    province_id: str,
    started_at: datetime,
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO public.tb_r_etl_run
                (pipeline_name, dag_run_id, task_id, province_id, started_at, status)
            VALUES (%s, %s, %s, %s, %s, 'running')
            RETURNING etl_run_id
            """,
            (pipeline_name, dag_run_id, task_id, int(province_id), started_at),
        )
        return cursor.fetchone()[0]


def finish_etl_run(
    connection: Any,
    etl_run_id: int,
    status: str,
    finished_at: datetime,
    counts: Optional[dict[str, int]] = None,
    error_message: Optional[str] = None,
) -> None:
    counts = counts or {}
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE public.tb_r_etl_run
            SET finished_at = %s,
                status = %s,
                weather_count = %s,
                hotspot_count = %s,
                air_quality_count = %s,
                forecast_count = %s,
                error_message = %s
            WHERE etl_run_id = %s
            """,
            (
                finished_at,
                status,
                counts.get("weather", 0),
                counts.get("hotspots", 0),
                counts.get("air_quality", 0),
                counts.get("forecast", 0),
                error_message,
                etl_run_id,
            ),
        )


def save_region_data(connection: Any, province: dict[str, Any], region: dict[str, Any], weather: dict[str, Any], hotspots: dict[str, Any], air_quality: dict[str, Any], forecast: dict[str, Any]) -> dict[str, int]:
    region_id = region["id"]
    _upsert(connection, """INSERT INTO public.tb_m_province (province_id,province_name,population,total_area_km2,latitude,longitude,timezone) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (province_id) DO UPDATE SET province_name=EXCLUDED.province_name,population=EXCLUDED.population,total_area_km2=EXCLUDED.total_area_km2,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,timezone=EXCLUDED.timezone""", [(int(province["id"]),province["name"],province["population"],province["total_area_km2"],province["latitude"],province["longitude"],province["timezone"])])
    _upsert(connection, """INSERT INTO public.tb_r_regency_city (regency_city_id,province_id,regency_city_name,population,total_area_km2,latitude,longitude,timezone) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (regency_city_id) DO UPDATE SET province_id=EXCLUDED.province_id,regency_city_name=EXCLUDED.regency_city_name,population=EXCLUDED.population,total_area_km2=EXCLUDED.total_area_km2,latitude=EXCLUDED.latitude,longitude=EXCLUDED.longitude,timezone=EXCLUDED.timezone""", [(region_id,int(province["id"]),region["name"],region["population"],region["total_area_km2"],region["latitude"],region["longitude"],region["timezone"])])
    weather_count = _upsert(connection, """INSERT INTO public.tb_r_weather (regency_city_id,weather,measured_at,temperature,humidity,precipitation_mm,wind_speed_kmh,wind_direction,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (regency_city_id,measured_at,source_name) DO UPDATE SET weather=EXCLUDED.weather,temperature=EXCLUDED.temperature,humidity=EXCLUDED.humidity,precipitation_mm=EXCLUDED.precipitation_mm,wind_speed_kmh=EXCLUDED.wind_speed_kmh,wind_direction=EXCLUDED.wind_direction""", ((region_id,item["weather"],datetime.combine(date.fromisoformat(item["time"]),datetime.min.time()),item["temperature_c"],item["humidity_pct"],item["precipitation_mm"],item["wind_speed_kmh"],item["wind_direction"],weather["source"]) for item in weather["data"]))
    hotspot_count = _upsert(connection, """INSERT INTO public.tb_r_hotspot (regency_city_id,latitude,longitude,detected_at,confidence,frp_mw,daynight,sensor,brightness_k,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (regency_city_id,detected_at,latitude,longitude,source_name) DO UPDATE SET confidence=EXCLUDED.confidence,frp_mw=EXCLUDED.frp_mw,daynight=EXCLUDED.daynight,sensor=EXCLUDED.sensor,brightness_k=EXCLUDED.brightness_k""", ((region_id,item["latitude"],item["longitude"],_utc(item["time"]),item["confidence"],item["frp_mw"],item["daynight"],hotspots["sensor"],item["brightness_k"],hotspots["source"]) for item in hotspots["data"]))
    aq_count = _upsert(connection, """INSERT INTO public.tb_r_air_quality (regency_city_id,measured_at,pm25,pm10,co,no2,aqi,dust_ugm3,aerosol_optical_depth,uv_index,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (regency_city_id,measured_at,source_name) DO UPDATE SET pm25=EXCLUDED.pm25,pm10=EXCLUDED.pm10,co=EXCLUDED.co,no2=EXCLUDED.no2,aqi=EXCLUDED.aqi,dust_ugm3=EXCLUDED.dust_ugm3,aerosol_optical_depth=EXCLUDED.aerosol_optical_depth,uv_index=EXCLUDED.uv_index""", ((region_id,_air_quality_time(item["time"],air_quality["period"]["timezone"]),item["pm2_5_ugm3"],item["pm10_ugm3"],item["carbon_monoxide_ugm3"],item["nitrogen_dioxide_ugm3"],item["us_aqi"],item["dust_ugm3"],item["aerosol_optical_depth"],item["uv_index"],air_quality["source"]) for item in air_quality["data"]))
    forecast_count = _upsert(connection, """INSERT INTO public.tb_r_weather_forecast (regency_city_id,forecast_date,issued_at,weather,weather_code,temperature_min_c,temperature_max_c,temperature_avg_c,humidity_avg_pct,precipitation_mm,precipitation_probability_max_pct,wind_speed_max_kmh,wind_direction_deg,wind_direction,uv_index_max,source_name) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (regency_city_id,forecast_date,source_name) DO UPDATE SET issued_at=EXCLUDED.issued_at,weather=EXCLUDED.weather,weather_code=EXCLUDED.weather_code,temperature_min_c=EXCLUDED.temperature_min_c,temperature_max_c=EXCLUDED.temperature_max_c,temperature_avg_c=EXCLUDED.temperature_avg_c,humidity_avg_pct=EXCLUDED.humidity_avg_pct,precipitation_mm=EXCLUDED.precipitation_mm,precipitation_probability_max_pct=EXCLUDED.precipitation_probability_max_pct,wind_speed_max_kmh=EXCLUDED.wind_speed_max_kmh,wind_direction_deg=EXCLUDED.wind_direction_deg,wind_direction=EXCLUDED.wind_direction,uv_index_max=EXCLUDED.uv_index_max""", ((region_id,item["forecast_date"],forecast["issued_at"],item["weather"],item["weather_code"],item["temperature_min_c"],item["temperature_max_c"],item["temperature_avg_c"],item["humidity_avg_pct"],item["precipitation_mm"],item["precipitation_probability_max_pct"],item["wind_speed_max_kmh"],item["wind_direction_deg"],item["wind_direction"],item["uv_index_max"],forecast["source"]) for item in forecast["data"]))
    return {"weather":weather_count,"hotspots":hotspot_count,"air_quality":aq_count,"forecast":forecast_count}
