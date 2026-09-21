from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from app.services.regions.service import EmsifaError, get_provinces_with_regencies

router = APIRouter(prefix="/api/v1", tags=["provinces"])


@router.get("/provinces")
async def provinces(
    keyword: Optional[str] = Query(None, min_length=2, description="Kata kunci nama provinsi, misalnya jakarta"),
) -> dict[str, Any]:
    try:
        return await get_provinces_with_regencies(keyword)
    except EmsifaError as exc:
        raise HTTPException(status_code=502, detail="Data wilayah tidak dapat dimuat.") from exc
