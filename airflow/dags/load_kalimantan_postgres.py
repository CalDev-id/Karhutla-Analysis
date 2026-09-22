import asyncio
import logging
import sys
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

PROJECT_ROOT = "/opt/airflow/project"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.database.postgres.connection import connection
from app.database.postgres.loaders import create_etl_run, finish_etl_run
from app.pipelines.postgres.load import load_province_data

logger = logging.getLogger(__name__)
JAKARTA = "Asia/Jakarta"
KALIMANTAN_PROVINCES = ("61", "62", "63", "64", "65")


@dag(
    dag_id="load_kalimantan_postgres",
    description="Memuat weather, hotspot, air quality, dan forecast provinsi Kalimantan ke PostgreSQL.",
    schedule="0 9 * * *",
    start_date=pendulum.datetime(2026, 9, 22, tz=JAKARTA),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=15)},
    tags=["karhutla", "postgres", "kalimantan"],
)
def load_kalimantan_postgres():
    @task
    def load_province(province_id: str) -> dict[str, object]:
        context = get_current_context()
        etl_run_id = None
        try:
            with connection() as database_connection:
                etl_run_id = create_etl_run(
                    database_connection,
                    pipeline_name="load_kalimantan_postgres",
                    dag_run_id=context["run_id"],
                    task_id=context["task"].task_id,
                    province_id=province_id,
                    started_at=pendulum.now("UTC"),
                )

            end_date = pendulum.now(JAKARTA).subtract(days=1).date()
            loaded = asyncio.run(load_province_data(province_id, end_date, end_date, days=2))
            result = {
                "province_id": province_id,
                "weather": loaded["totals"]["weather"],
                "hotspots": loaded["totals"]["hotspots"],
                "air_quality": loaded["totals"]["air_quality"],
                "forecast": loaded["totals"]["forecast"],
            }
            with connection() as database_connection:
                finish_etl_run(database_connection, etl_run_id, "success", pendulum.now("UTC"), result)
            logger.info("Pipeline provinsi selesai: %s", result)
            return result
        except Exception as exc:
            if etl_run_id is not None:
                try:
                    with connection() as database_connection:
                        finish_etl_run(
                            database_connection,
                            etl_run_id,
                            "failed",
                            pendulum.now("UTC"),
                            error_message=str(exc)[:2_000],
                        )
                except Exception:
                    logger.exception("Gagal mencatat kegagalan ETL %s", province_id)
            raise

    previous_task = None
    for province_id in KALIMANTAN_PROVINCES:
        current_task = load_province.override(task_id="load_province_" + province_id)(province_id)
        if previous_task is not None:
            previous_task >> current_task
        previous_task = current_task


load_kalimantan_postgres()
