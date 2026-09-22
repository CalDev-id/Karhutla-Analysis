CREATE TABLE IF NOT EXISTS public.tb_r_etl_run (
    etl_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    dag_run_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    province_id INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    weather_count INTEGER NOT NULL DEFAULT 0,
    hotspot_count INTEGER NOT NULL DEFAULT 0,
    air_quality_count INTEGER NOT NULL DEFAULT 0,
    forecast_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_run_pipeline_started
    ON public.tb_r_etl_run (pipeline_name, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_run_province_started
    ON public.tb_r_etl_run (province_id, started_at DESC);
