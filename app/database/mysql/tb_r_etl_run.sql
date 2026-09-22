CREATE TABLE IF NOT EXISTS tb_r_etl_run (
    etl_run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    pipeline_name VARCHAR(100) NOT NULL,
    dag_run_id VARCHAR(250) NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    province_id INT NOT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status ENUM('running', 'success', 'failed') NOT NULL,
    weather_count INT NOT NULL DEFAULT 0,
    hotspot_count INT NOT NULL DEFAULT 0,
    air_quality_count INT NOT NULL DEFAULT 0,
    forecast_count INT NOT NULL DEFAULT 0,
    error_message TEXT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (etl_run_id),
    KEY idx_etl_run_pipeline_started (pipeline_name, started_at),
    KEY idx_etl_run_province_started (province_id, started_at)
);
