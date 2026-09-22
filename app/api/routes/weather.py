from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.weather.historical import (
    VisualCrossingConfigurationError,
    VisualCrossingError,
    get_history,
)

router = APIRouter(prefix="/api/v1", tags=["weather"])

@router.get("/weather/history")
async def weather_history(
    location: str = Query(..., min_length=2, max_length=200, description="Nama tempat, kota, atau koordinat"),
    start_date: date = Query(..., description="Tanggal awal, format YYYY-MM-DD"),
    end_date: date = Query(..., description="Tanggal akhir, format YYYY-MM-DD"),
) -> dict[str, Any]:
    if end_date < start_date:
        raise HTTPException(status_code=422, detail="end_date harus sama atau setelah start_date.")
    if (end_date - start_date).days + 1 > 30:
        raise HTTPException(status_code=422, detail="Rentang tanggal maksimal 30 hari.")
    if end_date > date.today():
        raise HTTPException(status_code=422, detail="end_date tidak boleh melewati hari ini.")
    try:
        return await get_history(location, start_date, end_date)
    except VisualCrossingConfigurationError as exc:
        raise HTTPException(status_code=500, detail="Visual Crossing belum dikonfigurasi.") from exc
    except VisualCrossingError as exc:
        raise HTTPException(status_code=502, detail="Visual Crossing tidak menyediakan data yang dapat digunakan.") from exc
