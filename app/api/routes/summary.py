from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.fire.service import FIRMSConfigurationError, FIRMSError
from app.services.regions.service import EmsifaError, RegionNotFoundError
from app.services.summary.service import get_regional_summary
from app.services.weather.historical import VisualCrossingConfigurationError, VisualCrossingError

router = APIRouter(prefix="/api/v1/summary", tags=["summary"])


@router.get("/{region_id}")
async def regional_summary(region_id: str, days: int = Query(1, ge=1, le=5)) -> dict[str, Any]:
    try:
        return await get_regional_summary(region_id, days)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RegionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Wilayah tidak ditemukan.") from exc
    except EmsifaError as exc:
        raise HTTPException(status_code=502, detail="Batas administrasi tidak dapat dimuat.") from exc
    except FIRMSConfigurationError as exc:
        raise HTTPException(status_code=500, detail="NASA FIRMS belum dikonfigurasi.") from exc
    except VisualCrossingConfigurationError as exc:
        raise HTTPException(status_code=500, detail="Visual Crossing belum dikonfigurasi.") from exc
    except (FIRMSError, VisualCrossingError) as exc:
        raise HTTPException(status_code=502, detail="Data ringkasan wilayah tidak dapat dimuat.") from exc
