from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from app.database.postgres.connection import PostgreSQLConnectionError, connection

router = APIRouter(prefix="/api/v1/overview", tags=["overview"])


def _number(value: Any) -> Optional[float]:
    return float(value) if isinstance(value, Decimal) else value


@router.get("/{region_id}")
async def regional_overview(region_id: str, days: int = Query(1, ge=1, le=5)) -> dict[str, Any]:
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    end_exclusive = end_date + timedelta(days=1)

    try:
        with connection() as database_connection:
            with database_connection.cursor() as cursor:
                cursor.execute("""
                    SELECT RTRIM(r.regency_city_id), r.regency_city_name, r.latitude, r.longitude, r.timezone,
                           r.province_id, p.province_name
                    FROM public.tb_r_regency_city r
                    JOIN public.tb_m_province p ON p.province_id = r.province_id
                    WHERE r.regency_city_id = %s
                """, (region_id,))
                row = cursor.fetchone()
                if row is None:
                    raise HTTPException(status_code=404, detail="Kabupaten/kota belum tersedia di PostgreSQL.")

                region = {
                    "id": row[0], "name": row[1], "latitude": _number(row[2]),
                    "longitude": _number(row[3]), "timezone": row[4],
                    "province_id": row[5], "province_name": row[6],
                }
                cursor.execute("""
                    SELECT measured_at, weather, temperature, humidity, precipitation_mm, wind_speed_kmh, wind_direction
                    FROM public.tb_r_weather
                    WHERE regency_city_id = %s AND measured_at >= %s AND measured_at < %s
                    ORDER BY measured_at
                """, (region_id, start_date, end_exclusive))
                weather = [
                    {
                        "time": weather_row[0].date().isoformat(), "weather": weather_row[1],
                        "temperature_c": _number(weather_row[2]), "humidity_pct": _number(weather_row[3]),
                        "precipitation_mm": _number(weather_row[4]), "wind_speed_kmh": _number(weather_row[5]),
                        "wind_direction": weather_row[6],
                    }
                    for weather_row in cursor.fetchall()
                ]
                cursor.execute("""
                    SELECT COUNT(*), AVG(frp_mw), MAX(frp_mw), MAX(detected_at)
                    FROM public.tb_r_hotspot
                    WHERE regency_city_id = %s AND detected_at >= %s AND detected_at < %s
                """, (region_id, start_date, end_exclusive))
                hotspot = cursor.fetchone()

        return {
            "region": region,
            "period": {"days": days, "start": start_date.isoformat(), "end": end_date.isoformat()},
            "weather": {"source": "PostgreSQL", "data": weather},
            "hotspots": {
                "source": "PostgreSQL", "count": hotspot[0],
                "average_frp_mw": _number(hotspot[1]), "max_frp_mw": _number(hotspot[2]),
                "latest_time": hotspot[3].isoformat() + "Z" if isinstance(hotspot[3], datetime) else None,
            },
        }
    except PostgreSQLConnectionError as exc:
        raise HTTPException(status_code=502, detail="PostgreSQL tidak dapat diakses.") from exc
