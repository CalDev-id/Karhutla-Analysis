import csv
from io import StringIO
from typing import Any

import httpx

from app.config import environment
from app.services.fire.transform import bounds, clean_hotspots
from app.services.regions.service import EmsifaError, get_region_paths

FIRMS_AREA_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
SENSOR = "VIIRS_SNPP_NRT"
TIMEOUT_SECONDS = 20.0


class FIRMSConfigurationError(Exception):
    """The NASA FIRMS MAP_KEY is missing."""


class FIRMSError(Exception):
    """NASA FIRMS data cannot be retrieved or parsed."""


def _map_key() -> str:
    map_key = environment("MAP_KEY")
    if not map_key:
        raise FIRMSConfigurationError
    return map_key


async def get_hotspots(days: int, region: dict[str, Any], limit: int) -> dict[str, Any]:
    try:
        paths = await get_region_paths(region["id"])
    except EmsifaError as exc:
        raise FIRMSError from exc

    west, south, east, north = bounds(paths)
    url = f"{FIRMS_AREA_URL}/{_map_key()}/{SENSOR}/{west},{south},{east},{north}/{days}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.get(url)
            response.raise_for_status()
        rows = list(csv.DictReader(StringIO(response.text)))
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, csv.Error) as exc:
        raise FIRMSError from exc

    data = clean_hotspots(rows, SENSOR, region, paths)
    page = data[:limit]
    return {
        "source": "NASA FIRMS",
        "region": {"id": region["id"], "name": region["name"], "level": region["level"]},
        "administrative_boundary_source": "Emsifa",
        "sensor": SENSOR,
        "days": days,
        "total_count": len(data),
        "count": len(page),
        "limit": limit,
        "data": page,
    }
