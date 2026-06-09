CREATE TABLE IF NOT EXISTS weather_data (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50)   NOT NULL,
    fetched_at      TIMESTAMP     NOT NULL,
    temperature_c   FLOAT,
    humidity_pct    INT,
    wind_speed_kmh  FLOAT,
    weather_code    INT,
    inserted_at     TIMESTAMP     DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id            SERIAL PRIMARY KEY,
    dag_run_id    VARCHAR(200),
    cities_count  INT,
    status        VARCHAR(20),
    executed_at   TIMESTAMP DEFAULT NOW()
);
