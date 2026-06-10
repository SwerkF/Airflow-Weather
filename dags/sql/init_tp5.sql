CREATE TABLE IF NOT EXISTS weather_raw_archive (
    id          SERIAL PRIMARY KEY,
    dag_run_id  VARCHAR(200) NOT NULL,
    city        VARCHAR(50)  NOT NULL,
    raw_json    JSONB        NOT NULL,
    archived_at TIMESTAMP    DEFAULT NOW(),
    UNIQUE (dag_run_id, city)
);

CREATE TABLE IF NOT EXISTS weather_observations (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50)  NOT NULL,
    fetched_at      TIMESTAMP    NOT NULL,
    temperature_c   FLOAT,
    humidity_pct    INT,
    wind_speed_kmh  FLOAT,
    weather_code    INT,
    dag_run_id      VARCHAR(200),
    inserted_at     TIMESTAMP    DEFAULT NOW(),
    UNIQUE (city, fetched_at)
);

CREATE TABLE IF NOT EXISTS quality_anomalies (
    id          SERIAL PRIMARY KEY,
    dag_run_id  VARCHAR(200) NOT NULL,
    city        VARCHAR(50)  NOT NULL,
    check_name  VARCHAR(100) NOT NULL,
    detail      TEXT,
    detected_at TIMESTAMP    DEFAULT NOW(),
    UNIQUE (dag_run_id, city, check_name)
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id           SERIAL PRIMARY KEY,
    dag_run_id   VARCHAR(200) UNIQUE NOT NULL,
    cities_count INT,
    status       VARCHAR(30),
    details      TEXT,
    executed_at  TIMESTAMP    DEFAULT NOW()
);

ALTER TABLE ingestion_log ADD COLUMN IF NOT EXISTS details TEXT;
