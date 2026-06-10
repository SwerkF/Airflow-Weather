import json
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"


def archive_raw(dag_run_id: str, city: str, raw_data: dict) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        """
        INSERT INTO weather_raw_archive (dag_run_id, city, raw_json)
        VALUES (%s, %s, %s)
        ON CONFLICT (dag_run_id, city)
            DO UPDATE SET raw_json = EXCLUDED.raw_json, archived_at = NOW()
        """,
        parameters=(dag_run_id, city, json.dumps(raw_data)),
    )
    
    logger.info(f"[{city}] Données brutes archivées (run: {dag_run_id})")
