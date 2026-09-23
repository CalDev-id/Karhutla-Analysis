import asyncio
import logging
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable

PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.pipelines.combined.load import load_province_data

logger = logging.getLogger(__name__)
JAKARTA = "Asia/Jakarta"
KALIMANTAN_PROVINCES = ("61", "62", "63", "64", "65")


@dag(
    dag_id="load_kalimantan_combined",
    description="Memuat master region ke MySQL dan environmental conditions ke PostgreSQL.",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 9, 22, tz=JAKARTA),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["karhutla", "mysql", "postgres", "kalimantan"],
)
def load_kalimantan_combined():
    @task
    def load_province(province_id: str) -> dict[str, object]:
        days = int(Variable.get("KARHUTLA_ETL_DAYS", default_var="5"))
        if not 1 <= days <= 5:
            raise ValueError("Airflow Variable KARHUTLA_ETL_DAYS harus bernilai 1 sampai 5.")
        end_date = pendulum.now(JAKARTA).date()
        start_date = end_date.subtract(days=days - 1)
        loaded = asyncio.run(load_province_data(province_id, start_date, end_date, days=days))
        environmental = loaded["environmental_conditions"]
        result = {
            "province_id": province_id,
            "region": loaded["region"],
            "weather": environmental["totals"]["weather"],
            "hotspots": environmental["totals"]["hotspots"],
            "air_quality": environmental["totals"]["air_quality"],
            "forecast": environmental["totals"]["forecast"],
        }
        logger.info("Pipeline gabungan provinsi selesai: %s", result)
        return result

    previous_task = None
    for province_id in KALIMANTAN_PROVINCES:
        current_task = load_province.override(task_id="load_province_" + province_id)(province_id)
        if previous_task is not None:
            previous_task >> current_task
        previous_task = current_task


load_kalimantan_combined()
