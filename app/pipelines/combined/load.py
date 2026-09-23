import asyncio
from datetime import date
from typing import Any

from app.database.mysql.connection import connection as mysql_connection
from app.database.mysql.loaders import save_region_master_data
from app.database.postgres.environment_conditions_connection import (
    connection as postgres_connection,
)
from app.database.postgres.environment_conditions_loaders import save_environmental_data
from app.services.air_quality.service import get_air_quality
from app.services.fire.service import get_hotspots
from app.services.regions.service import get_regencies, get_region
from app.services.weather.forecast import get_forecast
from app.services.weather.historical import get_history

MYSQL_REGION_DATABASE = "region"


async def load_province_data(
    province_id: str, start_date: date, end_date: date, days: int
) -> dict[str, Any]:
    """Load a province into the databases that own each data domain.

    The MySQL ``region`` database owns province/regency master data. PostgreSQL
    ``environment_conditions`` owns the four environmental datasets and does
    not contain master-region tables.
    """
    province = await get_region(province_id)
    if province["level"] != "province":
        raise ValueError("ETL data hanya mendukung province_id provinsi (NN).")

    regency_summaries = (await get_regencies(province_id))["data"]
    regencies = list(
        await asyncio.gather(*(get_region(regency["id"]) for regency in regency_summaries))
    )

    with mysql_connection() as database_connection:
        with database_connection.cursor() as cursor:
            cursor.execute(f"USE `{MYSQL_REGION_DATABASE}`")
        region_loaded = save_region_master_data(database_connection, province, regencies)

    environmental_loaded = []
    for region in regencies:
        location = f"{region['latitude']},{region['longitude']}"
        weather, hotspots, air_quality, forecast = await asyncio.gather(
            get_history(location, start_date, end_date),
            get_hotspots(days, region, limit=1_000_000),
            get_air_quality(region, days),
            get_forecast(region),
        )
        with postgres_connection() as database_connection:
            loaded = save_environmental_data(
                database_connection, region, weather, hotspots, air_quality, forecast
            )
        environmental_loaded.append({"region_id": region["id"], "loaded": loaded})

    fields = ("weather", "hotspots", "air_quality", "forecast")
    return {
        "region": region_loaded,
        "environmental_conditions": {
            "total_regencies": len(environmental_loaded),
            "totals": {
                field: sum(item["loaded"][field] for item in environmental_loaded)
                for field in fields
            },
            "data": environmental_loaded,
        },
    }
