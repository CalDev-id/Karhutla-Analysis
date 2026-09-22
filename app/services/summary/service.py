from asyncio import gather
from datetime import date, timedelta
from typing import Any

from app.services.fire.service import get_hotspots
from app.services.fire.transform import summarize_hotspots
from app.services.regions.service import EmsifaError, get_region
from app.services.weather.historical import get_history


async def get_regional_summary(region_id: str, days: int) -> dict[str, Any]:
    region = await get_region(region_id)
    if not region["has_path"]:
        raise EmsifaError
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)
    weather, hotspots = await gather(
        get_history(f"{region['latitude']},{region['longitude']}", start_date, end_date),
        get_hotspots(days, region, limit=1_000_000),
    )
    return {
        "region": region,
        "period": {"days": days, "start": start_date.isoformat(), "end": end_date.isoformat()},
        "weather": {"source": weather["source"], "data": weather["data"]},
        "hotspots": {"source": hotspots["source"], **summarize_hotspots(hotspots["data"])},
    }
