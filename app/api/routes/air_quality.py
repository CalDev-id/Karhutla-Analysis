from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.air_quality.service import OpenMeteoError, get_air_quality
from app.services.regions.service import EmsifaError, RegionNotFoundError, get_region

router = APIRouter(prefix="/api/v1/air-quality", tags=["air-quality"])


@router.get("")
async def air_quality(
    region_id: str = Query(..., pattern=r"^\d{2}\.\d{2}$", description="Kode kabupaten/kota, misalnya 61.01"),
    days: int = Query(1, ge=1, le=92, description="Hari terakhir"),
) -> dict[str, Any]:
    try:
        region = await get_region(region_id)
        return await get_air_quality(region, days)
    except RegionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Kabupaten/kota tidak ditemukan.") from exc
    except EmsifaError as exc:
        raise HTTPException(status_code=502, detail="Data wilayah tidak dapat dimuat.") from exc
    except OpenMeteoError as exc:
        raise HTTPException(status_code=502, detail="Data kualitas udara tidak dapat dimuat.") from exc
