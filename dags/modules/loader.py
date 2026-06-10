import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"

def load_weather_observation(city: str, data: dict, dag_run_id: str) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO weather_observations
            (city, fetched_at, temperature_c, humidity_pct, wind_speed_kmh, weather_code, dag_run_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, fetched_at) DO UPDATE SET
            temperature_c  = EXCLUDED.temperature_c,
            humidity_pct   = EXCLUDED.humidity_pct,
            wind_speed_kmh = EXCLUDED.wind_speed_kmh,
            weather_code   = EXCLUDED.weather_code,
            dag_run_id     = EXCLUDED.dag_run_id,
            inserted_at    = NOW()
        """,
        parameters=(
            data["city"],
            data["fetched_at"],
            data["temperature_c"],
            data["humidity_pct"],
            data["wind_speed_kmh"],
            data["weather_code"],
            dag_run_id,
        ),
    )

    logger.info(f"[{city}] Chargement OK — run : {dag_run_id}")


def save_quality_anomaly(dag_run_id: str, city: str, check_name: str, detail: str) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO quality_anomalies (dag_run_id, city, check_name, detail)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (dag_run_id, city, check_name) DO NOTHING
        """,
        parameters=(dag_run_id, city, check_name, detail),
    )


def write_ingestion_log(dag_run_id: str, cities_count: int, status: str, details: str = "") -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        "DELETE FROM ingestion_log WHERE dag_run_id = %s",
        parameters=(dag_run_id,),
    )
    
    hook.run(
        "INSERT INTO ingestion_log (dag_run_id, cities_count, status, details) VALUES (%s, %s, %s, %s)",
        parameters=(dag_run_id, cities_count, status, details),
    )
    logger.info(f"[INGESTION LOG] run={dag_run_id} | status={status} | villes={cities_count}")
