from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.fire.hotspots import FIRMSConfigurationError, FIRMSError, get_hotspots
from app.services.regions.service import EmsifaError, RegionNotFoundError, find_province, get_region

router = APIRouter(prefix="/api/v1/fire", tags=["fire"])


@router.get("/hotspots")
async def hotspots(
    days: int = Query(1, ge=1, le=5, description="Jumlah hari terakhir"),
    province: Optional[str] = Query(None, description="Nama provinsi, misalnya Riau"),
    region_id: Optional[str] = Query(None, pattern=r"^\d{2}(?:\.\d{2})?$", description="Kode provinsi atau kabupaten/kota"),
    limit: int = Query(100, ge=1, le=1_000, description="Maksimum titik yang dikembalikan"),
) -> dict[str, Any]:
    if (province is None and region_id is None) or (province is not None and region_id is not None):
        raise HTTPException(status_code=422, detail="Isi tepat satu: province atau region_id.")
    try:
        region = await get_region(region_id) if region_id else await find_province(province or "")
        if not region["has_path"]:
            raise EmsifaError
        return await get_hotspots(days, region, limit)
    except RegionNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Wilayah tidak ditemukan.") from exc
    except EmsifaError as exc:
        raise HTTPException(status_code=502, detail="Batas administrasi tidak dapat dimuat.") from exc
    except FIRMSConfigurationError as exc:
        raise HTTPException(status_code=500, detail="NASA FIRMS belum dikonfigurasi.") from exc
    except FIRMSError as exc:
        raise HTTPException(status_code=502, detail="NASA FIRMS tidak menyediakan data hotspot yang dapat digunakan.") from exc
