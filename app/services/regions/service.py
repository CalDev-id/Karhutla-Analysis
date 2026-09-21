import re
import time
from asyncio import gather
from typing import Any, Optional

import httpx

EMSIFA_BASE_URL = "https://www.emsifa.com/api-wilayah-indonesia/v2"
CACHE_TTL_SECONDS = 24 * 60 * 60
TIMEOUT_SECONDS = 15.0
REGION_ID_PATTERN = re.compile(r"^\d{2}(?:\.\d{2})?$")
_cache: dict[str, tuple[float, Any]] = {}


class EmsifaError(Exception):
    """Emsifa data cannot be retrieved or parsed."""


class RegionNotFoundError(Exception):
    """The requested region does not exist."""


def _cached(key: str) -> Optional[Any]:
    value = _cache.get(key)
    if value is None:
        return None
    expires_at, data = value
    return data if time.monotonic() < expires_at else None


async def _get_data(path: str) -> Any:
    cached_data = _cached(path)
    if cached_data is not None:
        return cached_data
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            response = await client.get(f"{EMSIFA_BASE_URL}/{path}")
            if response.status_code == 404:
                raise RegionNotFoundError
            response.raise_for_status()
            payload = response.json()
    except RegionNotFoundError:
        raise
    except (httpx.TimeoutException, httpx.RequestError, httpx.HTTPStatusError, ValueError) as exc:
        raise EmsifaError from exc
    if not isinstance(payload, dict) or "data" not in payload:
        raise EmsifaError
    _cache[path] = (time.monotonic() + CACHE_TTL_SECONDS, payload["data"])
    return payload["data"]


def _number(value: Any) -> Optional[float]:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _summary(item: Any) -> dict[str, str]:
    if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not isinstance(item.get("name"), str):
        raise EmsifaError
    return {"id": item["id"], "name": item["name"]}


def _timezone(value: Any) -> Optional[str]:
    hours = _number(value)
    if hours is None:
        return None
    sign = "+" if hours >= 0 else "-"
    return f"UTC{sign}{abs(hours):02.0f}:00"


def _master_fields(item: Any) -> dict[str, Any]:
    summary = _summary(item)
    if not isinstance(item, dict):
        raise EmsifaError
    population = item.get("population")
    return {
        "id": summary["id"],
        "name": summary["name"],
        "capital": item.get("capital") if isinstance(item.get("capital"), str) else None,
        "population": population if isinstance(population, int) and not isinstance(population, bool) else None,
        "total_area_km2": _number(item.get("total_area")),
        "latitude": _number(item.get("lat")),
        "longitude": _number(item.get("lng")),
        "timezone": _timezone(item.get("tz")),
    }


def _region_detail(item: Any, region_id: str) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise EmsifaError
    summary = _summary(item)
    latitude = _number(item.get("lat"))
    longitude = _number(item.get("lng"))
    has_path = item.get("has_path")
    if summary["id"] != region_id or latitude is None or longitude is None or not isinstance(has_path, bool):
        raise EmsifaError
    if "." in region_id:
        parent_id = _summary(item.get("province"))["id"]
        level = "regency_city"
    else:
        parent_id = None
        level = "province"
    return {
        "id": summary["id"], "name": summary["name"], "level": level,
        "parent_id": parent_id, "latitude": latitude, "longitude": longitude,
        "has_path": has_path, "capital": _master_fields(item)["capital"],
        "population": _master_fields(item)["population"],
        "total_area_km2": _master_fields(item)["total_area_km2"],
        "timezone": _master_fields(item)["timezone"],
    }


def _province_key(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())


async def get_provinces() -> dict[str, Any]:
    data = await _get_data("provinces.json")
    if not isinstance(data, list):
        raise EmsifaError
    return {"source": "Emsifa", "data": [_summary(item) for item in data]}


async def get_regencies(province_id: str) -> dict[str, Any]:
    provinces = await get_provinces()
    if not any(province["id"] == province_id for province in provinces["data"]):
        raise RegionNotFoundError
    data = await _get_data(f"regencies/{province_id}.json")
    if not isinstance(data, list):
        raise EmsifaError
    return {"source": "Emsifa", "data": [_summary(item) for item in data]}


async def get_provinces_with_regencies(keyword: Optional[str] = None) -> dict[str, Any]:
    provinces = await _get_data("provinces.json")
    if not isinstance(provinces, list):
        raise EmsifaError
    if keyword:
        province_keyword = _province_key(keyword)
        provinces = [
            province for province in provinces
            if isinstance(province, dict) and isinstance(province.get("name"), str)
            and province_keyword in _province_key(province["name"])
        ]
    regencies_by_province = await gather(
        *(_get_data(f"regencies/{_summary(province)['id']}.json") for province in provinces)
    )
    data = []
    for province, regencies in zip(provinces, regencies_by_province):
        province_fields = _master_fields(province)
        if not isinstance(regencies, list):
            raise EmsifaError
        data.append(
            {
                "province_id": province_fields["id"],
                "province_name": province_fields["name"],
                "capital": province_fields["capital"],
                "population": province_fields["population"],
                "total_area_km2": province_fields["total_area_km2"],
                "latitude": province_fields["latitude"],
                "longitude": province_fields["longitude"],
                "timezone": province_fields["timezone"],
                "regencies": [
                    {
                        "regency_id": regency_fields["id"],
                        "regency_name": regency_fields["name"],
                        "regency_type": "kota" if regency_fields["name"].lower().startswith("kota") else "kabupaten",
                        "capital": regency_fields["capital"],
                        "population": regency_fields["population"],
                        "total_area_km2": regency_fields["total_area_km2"],
                        "latitude": regency_fields["latitude"],
                        "longitude": regency_fields["longitude"],
                        "timezone": regency_fields["timezone"],
                    }
                    for regency_fields in (_master_fields(regency) for regency in regencies)
                ],
            }
        )
    return {"source": "Emsifa", "data": data}


async def get_region(region_id: str) -> dict[str, Any]:
    if not REGION_ID_PATTERN.fullmatch(region_id):
        raise ValueError("Format region_id harus NN atau NN.NN.")
    level = "regencies" if "." in region_id else "provinces"
    return _region_detail(await _get_data(f"{level}/{region_id}.json"), region_id)


async def find_province(name: str) -> dict[str, Any]:
    provinces = await get_provinces()
    for province in provinces["data"]:
        if _province_key(province["name"]) == _province_key(name):
            return await get_region(province["id"])
    raise RegionNotFoundError


async def get_region_paths(region_id: str) -> list[list[list[float]]]:
    data = await _get_data(f"paths/{region_id}.json")
    if not isinstance(data, dict) or data.get("id") != region_id or not isinstance(data.get("path"), list):
        raise EmsifaError
    raw_paths = data["path"]
    if raw_paths and isinstance(raw_paths[0], list) and len(raw_paths[0]) == 2 and isinstance(raw_paths[0][0], (int, float)):
        raw_paths = [raw_paths]
    paths = []
    for ring in raw_paths:
        if not isinstance(ring, list) or len(ring) < 4:
            continue
        coordinates = []
        for point in ring:
            if not isinstance(point, list) or len(point) != 2:
                coordinates = []
                break
            latitude, longitude = _number(point[0]), _number(point[1])
            if longitude is None or latitude is None:
                coordinates = []
                break
            coordinates.append([longitude, latitude])
        if coordinates:
            paths.append(coordinates)
    if not paths:
        raise EmsifaError
    return paths
