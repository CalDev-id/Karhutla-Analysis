from typing import Union

from fastapi import FastAPI

from app.api.routes.air_quality import router as air_quality_router
from app.api.routes.etl_mysql import router as etl_mysql_router
from app.api.routes.etl_postgres import router as etl_postgres_router
from app.api.routes.fire import router as fire_router
from app.api.routes.provinces import router as provinces_router
from app.api.routes.overview import router as overview_router
from app.api.routes.weather import router as weather_router

app = FastAPI()
#overview
app.include_router(overview_router)

#post ke database
app.include_router(etl_mysql_router)
app.include_router(etl_postgres_router)

#data services
app.include_router(air_quality_router)
app.include_router(fire_router)
app.include_router(provinces_router)
app.include_router(weather_router)

@app.get("/")
async def read_root() -> dict[str, Union[str, int]]:
    return {"message": "Welcome to the API Karhutla by Team Data Kementrian Sekretariat Negara", "status": 200}


# run
# uvicorn main:app --host 127.0.0.1 --port 8002
