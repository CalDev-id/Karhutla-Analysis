"""Loaders for the environmental-only PostgreSQL database."""

from datetime import date, datetime
from typing import Any

from app.database.postgres.loaders import _air_quality_time, _upsert, _utc


def save_environmental_data(
    connection: Any,
    region: dict[str, Any],
    weather: dict[str, Any],
    hotspots: dict[str, Any],
    air_quality: dict[str, Any],
    forecast: dict[str, Any],
) -> dict[str, int]:
    """Insert immutable environmental snapshots into ``environment_conditions``."""
    region_id = region["id"]
    weather_count = _upsert(
        connection,
        """
        INSERT INTO public.tb_r_weather
            (regency_city_id, weather, measured_at, temperature, humidity,
             precipitation_mm, wind_speed_kmh, wind_direction, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            (
                region_id,
                item["weather"],
                datetime.combine(date.fromisoformat(item["time"]), datetime.min.time()),
                item["temperature_c"],
                item["humidity_pct"],
                item["precipitation_mm"],
                item["wind_speed_kmh"],
                item["wind_direction"],
                weather["source"],
            )
            for item in weather["data"]
        ),
    )
    hotspot_count = _upsert(
        connection,
        """
        INSERT INTO public.tb_r_hotspot
            (regency_city_id, latitude, longitude, detected_at, confidence, frp_mw,
             daynight, sensor, brightness_k, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            (
                region_id,
                item["latitude"],
                item["longitude"],
                _utc(item["time"]),
                item["confidence"],
                item["frp_mw"],
                item["daynight"],
                hotspots["sensor"],
                item["brightness_k"],
                hotspots["source"],
            )
            for item in hotspots["data"]
        ),
    )
    air_quality_count = _upsert(
        connection,
        """
        INSERT INTO public.tb_r_air_quality
            (regency_city_id, measured_at, pm25, pm10, co, no2, aqi, dust_ugm3,
             aerosol_optical_depth, uv_index, source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            (
                region_id,
                _air_quality_time(item["time"], air_quality["period"]["timezone"]),
                item["pm2_5_ugm3"],
                item["pm10_ugm3"],
                item["carbon_monoxide_ugm3"],
                item["nitrogen_dioxide_ugm3"],
                item["us_aqi"],
                item["dust_ugm3"],
                item["aerosol_optical_depth"],
                item["uv_index"],
                air_quality["source"],
            )
            for item in air_quality["data"]
        ),
    )
    forecast_count = _upsert(
        connection,
        """
        INSERT INTO public.tb_r_weather_forecast
            (regency_city_id, forecast_date, issued_at, weather, weather_code,
             temperature_min_c, temperature_max_c, temperature_avg_c,
             humidity_avg_pct, precipitation_mm, precipitation_probability_max_pct,
             wind_speed_max_kmh, wind_direction_deg, wind_direction, uv_index_max,
             source_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            (
                region_id,
                item["forecast_date"],
                forecast["issued_at"],
                item["weather"],
                item["weather_code"],
                item["temperature_min_c"],
                item["temperature_max_c"],
                item["temperature_avg_c"],
                item["humidity_avg_pct"],
                item["precipitation_mm"],
                item["precipitation_probability_max_pct"],
                item["wind_speed_max_kmh"],
                item["wind_direction_deg"],
                item["wind_direction"],
                item["uv_index_max"],
                forecast["source"],
            )
            for item in forecast["data"]
        ),
    )
    return {
        "weather": weather_count,
        "hotspots": hotspot_count,
        "air_quality": air_quality_count,
        "forecast": forecast_count,
    }
