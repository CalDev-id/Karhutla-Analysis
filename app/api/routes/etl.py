import logging
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
import pymysql
from fastapi import APIRouter, HTTPException, Path, Query

from app.database.mysql.connection import MySQLConfigurationError, MySQLConnectionError
from app.database.postgres.environment_conditions_connection import (
    EnvironmentConditionsConfigurationError,
    EnvironmentConditionsConnectionError,
)
from app.pipelines.combined.load import load_province_data
from app.services.air_quality.service import OpenMeteoError
from app.services.fire.service import FIRMSConfigurationError, FIRMSError
from app.services.regions.service import EmsifaError, RegionNotFoundError
from app.services.weather.forecast import OpenMeteoForecastError
from app.services.weather.historical import VisualCrossingConfigurationError, VisualCrossingError

router = APIRouter(prefix="/api/v1/etl", tags=["etl"])
logger = logging.getLogger(__name__)


def _recent_period(days: int) -> tuple[date, date]:
    end_date = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    return end_date - timedelta(days=days - 1), end_date


@router.post("/provinces/{province_id}")
async def load_province(
    province_id: str = Path(..., pattern=r"^\d{2}$", description="Kode provinsi, misalnya 61"),
    days: int = Query(1, ge=1, le=5, description="Hari terakhir untuk seluruh data"),
) -> dict[str, Any]:
    start_date, end_date = _recent_period(days)
    try:
        loaded = await load_province_data(province_id, start_date, end_date, days)
    except RegionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Provinsi tidak ditemukan.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (EmsifaError, VisualCrossingError, FIRMSError, OpenMeteoError, OpenMeteoForecastError) as exc:
        raise HTTPException(status_code=502, detail="Salah satu sumber data tidak dapat dimuat.") from exc
    except (
        VisualCrossingConfigurationError,
        FIRMSConfigurationError,
        MySQLConfigurationError,
        EnvironmentConditionsConfigurationError,
    ) as exc:
        raise HTTPException(status_code=500, detail="Konfigurasi pipeline belum lengkap.") from exc
    except (MySQLConnectionError, EnvironmentConditionsConnectionError, pymysql.MySQLError, psycopg2.Error) as exc:
        logger.exception("Database gagal saat ETL provinsi %s", province_id)
        raise HTTPException(status_code=502, detail="Salah satu database tidak dapat diakses.") from exc

    return {
        "status": "success",
        "province_id": province_id,
        "period": {"days": days, "start": start_date.isoformat(), "end": end_date.isoformat()},
        "loaded": loaded,
    }
