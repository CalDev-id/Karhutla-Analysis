import asyncio
from datetime import date
from typing import Any

from app.database.postgres.connection import connection
from app.database.postgres.loaders import save_region_data
from app.services.air_quality.service import get_air_quality
from app.services.fire.service import get_hotspots
from app.services.regions.service import get_regencies, get_region
from app.services.weather.historical import get_history
from app.services.weather.forecast import get_forecast


async def load_region_data(region_id: str, start_date: date, end_date: date, days: int) -> dict[str, int]:
    region = await get_region(region_id)
    if region["level"] != "regency_city":
        raise ValueError("ETL data hanya mendukung region_id kabupaten/kota (NN.NN).")
    province = await get_region(region["parent_id"])
    location = f"{region['latitude']},{region['longitude']}"
    weather, hotspots, air_quality, forecast = await asyncio.gather(
        get_history(location, start_date, end_date), get_hotspots(days, region, limit=1_000_000),
        get_air_quality(region, days), get_forecast(region),
    )
    with connection() as database_connection:
        return save_region_data(database_connection, province, region, weather, hotspots, air_quality, forecast)


async def load_province_data(province_id: str, start_date: date, end_date: date, days: int) -> dict[str, Any]:
    data = []
    for regency in (await get_regencies(province_id))["data"]:
        loaded = await load_region_data(regency["id"], start_date, end_date, days)
        data.append({"region_id": regency["id"], "region_name": regency["name"], "loaded": loaded})
    fields = ("weather", "hotspots", "air_quality", "forecast")
    return {"total_regencies": len(data), "totals": {field: sum(item["loaded"][field] for item in data) for field in fields}, "data": data}
