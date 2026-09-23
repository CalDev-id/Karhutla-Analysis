from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
import pymysql
from fastapi import APIRouter, HTTPException, Path, Query

from app.database.mysql.connection import MySQLConfigurationError, MySQLConnectionError, connection as mysql_connection
from app.database.postgres.environment_conditions_connection import (
    EnvironmentConditionsConfigurationError,
    EnvironmentConditionsConnectionError,
    connection as postgres_connection,
)

router = APIRouter(prefix="/api/v1/overview", tags=["environment-conditions"])
MYSQL_REGION_DATABASE = "region"


def _number(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


@router.get("/regions/{region_id}")
async def regional_environment_conditions(
    region_id: str = Path(..., pattern=r"^\d{2}\.\d{2}$", description="Kode kabupaten/kota, misalnya 61.01"),
    days: int = Query(1, ge=1, le=5, description="Hari terakhir untuk weather, hotspot, dan air quality"),
) -> dict[str, Any]:
    end_date = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    start_date = end_date - timedelta(days=days - 1)
    end_exclusive = end_date + timedelta(days=1)

    try:
        with mysql_connection() as database_connection:
            with database_connection.cursor() as cursor:
                cursor.execute(f"USE `{MYSQL_REGION_DATABASE}`")
                cursor.execute(
                    """
                    SELECT r.regency_city_id, r.regency_city_name, r.population, r.total_area_km2,
                           r.latitude, r.longitude, r.timezone, r.province_id, p.province_name
                    FROM tb_r_regency_city r
                    JOIN tb_m_province p ON p.province_id = r.province_id
                    WHERE r.regency_city_id = %s
                    """,
                    (region_id,),
                )
                row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Kabupaten/kota belum tersedia di MySQL region.")
        region = {
            "id": row[0], "name": row[1], "population": row[2], "total_area_km2": _number(row[3]),
            "latitude": _number(row[4]), "longitude": _number(row[5]), "timezone": row[6],
            "province_id": row[7], "province_name": row[8],
        }

        with postgres_connection() as database_connection:
            with database_connection.cursor() as cursor:
                cursor.execute("""SELECT measured_at, weather, temperature, humidity, precipitation_mm, wind_speed_kmh, wind_direction FROM public.tb_r_weather WHERE regency_city_id = %s AND measured_at >= %s AND measured_at < %s ORDER BY measured_at""", (region_id, start_date, end_exclusive))
                weather = [{"time": item[0].date().isoformat(), "weather": item[1], "temperature_c": _number(item[2]), "humidity_pct": _number(item[3]), "precipitation_mm": _number(item[4]), "wind_speed_kmh": _number(item[5]), "wind_direction": item[6]} for item in cursor.fetchall()]
                cursor.execute("""SELECT COUNT(*), AVG(frp_mw), MAX(frp_mw), MAX(detected_at) FROM public.tb_r_hotspot WHERE regency_city_id = %s AND detected_at >= %s AND detected_at < %s""", (region_id, start_date, end_exclusive))
                hotspot = cursor.fetchone()
    except (MySQLConfigurationError, EnvironmentConditionsConfigurationError) as exc:
        raise HTTPException(status_code=500, detail="Konfigurasi database belum lengkap.") from exc
    except (MySQLConnectionError, EnvironmentConditionsConnectionError, pymysql.MySQLError, psycopg2.Error) as exc:
        raise HTTPException(status_code=502, detail="Salah satu database tidak dapat diakses.") from exc

    return {
        "region": region,
        "period": {"days": days, "start": start_date.isoformat(), "end": end_date.isoformat()},
        "weather": {"source": "PostgreSQL", "data": weather},
        "hotspots": {
            "source": "PostgreSQL",
            "count": hotspot[0],
            "average_frp_mw": _number(hotspot[1]),
            "max_frp_mw": _number(hotspot[2]),
            "latest_time": hotspot[3].isoformat() + "Z" if isinstance(hotspot[3], datetime) else None,
        },
    }
