import logging
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
from fastapi import APIRouter, HTTPException, Path, Query

from app.database.postgres.connection import PostgreSQLConfigurationError, PostgreSQLConnectionError
from app.pipelines.postgres.load import load_province_data, load_region_data
from app.services.air_quality.service import OpenMeteoError
from app.services.fire.service import FIRMSConfigurationError, FIRMSError
from app.services.regions.service import EmsifaError, RegionNotFoundError
from app.services.weather.historical import VisualCrossingConfigurationError, VisualCrossingError
from app.services.weather.forecast import OpenMeteoForecastError

router = APIRouter(prefix="/api/v1/etl/postgres", tags=["etl-postgres"])
logger = logging.getLogger(__name__)


def _recent_period(days: int) -> tuple[date, date]:
    end_date = datetime.now(ZoneInfo("Asia/Jakarta")).date()
    return end_date - timedelta(days=days - 1), end_date


async def _run(region_or_province: str, days: int, is_province: bool) -> dict[str, Any]:
    start_date, end_date = _recent_period(days)
    try:
        loaded = (
            await load_province_data(region_or_province, start_date, end_date, days)
            if is_province else await load_region_data(region_or_province, start_date, end_date, days)
        )
    except RegionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Wilayah tidak ditemukan.") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except (EmsifaError, VisualCrossingError, FIRMSError, OpenMeteoError, OpenMeteoForecastError) as exc:
        raise HTTPException(status_code=502, detail="Salah satu sumber data tidak dapat dimuat.") from exc
    except (VisualCrossingConfigurationError, FIRMSConfigurationError, PostgreSQLConfigurationError) as exc:
        raise HTTPException(status_code=500, detail="Konfigurasi pipeline belum lengkap.") from exc
    except (PostgreSQLConnectionError, psycopg2.Error) as exc:
        logger.exception("PostgreSQL gagal saat ETL %s", region_or_province)
        raise HTTPException(status_code=502, detail="PostgreSQL tidak dapat diakses.") from exc
    return {"period": {"days": days, "start": start_date.isoformat(), "end": end_date.isoformat()}, "loaded": loaded}


@router.post("/regions/{region_id}")
async def load_region(
    region_id: str = Path(..., pattern=r"^\d{2}\.\d{2}$"),
    days: int = Query(1, ge=1, le=5),
) -> dict[str, Any]:
    result = await _run(region_id, days, is_province=False)
    return {"status": "success", "target": "PostgreSQL", "region_id": region_id, **result}


@router.post("/provinces/{province_id}")
async def load_province(
    province_id: str = Path(..., pattern=r"^\d{2}$"),
    days: int = Query(1, ge=1, le=5),
) -> dict[str, Any]:
    result = await _run(province_id, days, is_province=True)
    return {"status": "success", "target": "PostgreSQL", "province_id": province_id, **result}
