from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.archiver import archive_raw
from modules.extractor import fetch_city_weather
from modules.loader import load_weather_observation, save_quality_anomaly, write_ingestion_log
from modules.quality_checker import run_quality_checks
from modules.transformer import transform_weather

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"

CITIES: dict[str, dict[str, float]] = {
    "paris":    {"lat": 48.8566, "lon": 2.3522},
    "tokyo":    {"lat": 35.6762, "lon": 139.6503},
    "new_york": {"lat": 40.7128, "lon": -74.0060},
}


def _create_tables(**context) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with open("/opt/airflow/dags/sql/init_tp5.sql") as fh:
        hook.run(fh.read())
    logger.info("Tables créées ou déjà existantes")


def _extract_and_archive(city: str, lat: float, lon: float, **context) -> None:
    dag_run_id: str = context["dag_run"].run_id
    raw_data = fetch_city_weather(city, lat, lon)
    archive_raw(dag_run_id, city, raw_data)
    context["ti"].xcom_push(key=f"raw_{city}", value=raw_data)
    logger.info(f"[{city}] Extraction et archivage terminés")


def _transform(city: str, **context) -> None:
    raw_data: dict | None = context["ti"].xcom_pull(
        key=f"raw_{city}", task_ids=f"extract_weather_{city}"
    )
    if raw_data is None:
        raise ValueError(f"[{city}] Aucune donnée brute trouvée en XCom")

    force_anomaly: bool = city in context["params"].get("simulate_anomaly", [])
    processed = transform_weather(city, raw_data, force_anomaly=force_anomaly)
    context["ti"].xcom_push(key=f"processed_{city}", value=processed)


def _quality_check(city: str, **context) -> None:
    data: dict | None = context["ti"].xcom_pull(
        key=f"processed_{city}", task_ids=f"transform_{city}"
    )
    if data is None:
        raise ValueError(f"[{city}] Aucune donnée transformée trouvée en XCom")

    anomalies = run_quality_checks(city, data)
    quality_ok = len(anomalies) == 0

    context["ti"].xcom_push(key=f"quality_ok_{city}", value=quality_ok)
    context["ti"].xcom_push(key=f"anomalies_{city}", value=anomalies)

    if quality_ok:
        logger.info(f"[{city}] Contrôles qualité OK")
    else:
        logger.warning(f"[{city}] {len(anomalies)} anomalie(s) : {anomalies}")


def _quality_gate(**context) -> str | list[str]:
    cities = list(CITIES.keys())
    failed = [
        city for city in cities
        if not context["ti"].xcom_pull(
            key=f"quality_ok_{city}", task_ids=f"quality_check_{city}"
        )
    ]

    if not failed:
        logger.info("Qualité validée pour toutes les villes → chargement")
        return [f"load_weather_{city}" for city in cities]

    logger.warning(f"Anomalies détectées pour : {failed} → blocage du chargement")
    return "handle_quality_anomalies"


def _load_weather(city: str, **context) -> None:
    dag_run_id: str = context["dag_run"].run_id
    data: dict | None = context["ti"].xcom_pull(
        key=f"processed_{city}", task_ids=f"transform_{city}"
    )
    if data is None:
        raise ValueError(f"[{city}] Aucune donnée à charger")
    load_weather_observation(city, data, dag_run_id)


def _handle_quality_anomalies(**context) -> None:
    dag_run_id: str = context["dag_run"].run_id
    cities = list(CITIES.keys())

    logged: list[str] = []
    for city in cities:
        anomalies: list[dict] = context["ti"].xcom_pull(
            key=f"anomalies_{city}", task_ids=f"quality_check_{city}"
        ) or []
        for a in anomalies:
            save_quality_anomaly(dag_run_id, city, a["check_name"], a["detail"])
            logged.append(f"{city}:{a['check_name']}")

    write_ingestion_log(dag_run_id, len(cities), "quality_failure", str(logged))
    logger.error(f"[QUALITÉ] Chargement bloqué — anomalies tracées : {logged}")


def _log_ingestion_success(**context) -> None:
    dag_run_id: str = context["dag_run"].run_id
    cities = list(CITIES.keys())
    write_ingestion_log(dag_run_id, len(cities), "success", "Toutes les villes chargées")
    logger.info(f"[SUCCESS] {len(cities)} villes chargées — run : {dag_run_id}")


with DAG(
    dag_id="weather_pipeline_v5",
    description="Pipeline météo Open-Meteo — extraction, archivage, qualité, chargement idempotent",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["météo", "open-meteo", "postgresql", "tp5"],
    default_args={"owner": "data-team"},
    params={
        "cities": Param(
            default=["paris", "tokyo", "new_york"],
            type="array",
            description="Villes à traiter",
        ),
        "simulate_anomaly": Param(
            default=[],
            type="array",
            description="Villes pour lesquelles simuler une anomalie qualité (ex: ['paris'])",
        ),
    },
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    init_db = PythonOperator(
        task_id="init_db",
        python_callable=_create_tables,
    )

    quality_gate = BranchPythonOperator(
        task_id="quality_gate",
        python_callable=_quality_gate,
        trigger_rule="all_done",
    )

    join_load = EmptyOperator(
        task_id="join_load",
        trigger_rule="all_done",
    )

    log_ingestion_success = PythonOperator(
        task_id="log_ingestion_success",
        python_callable=_log_ingestion_success,
    )

    handle_quality_anomalies = PythonOperator(
        task_id="handle_quality_anomalies",
        python_callable=_handle_quality_anomalies,
    )

    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        trigger_rule="all_done",
    )

    start_pipeline >> init_db

    for city, coords in CITIES.items():
        extract = PythonOperator(
            task_id=f"extract_weather_{city}",
            python_callable=_extract_and_archive,
            op_kwargs={"city": city, "lat": coords["lat"], "lon": coords["lon"]},
            retries=3,
            retry_delay=timedelta(seconds=30),
            execution_timeout=timedelta(seconds=30),
        )

        transform = PythonOperator(
            task_id=f"transform_{city}",
            python_callable=_transform,
            op_kwargs={"city": city},
        )

        quality_check = PythonOperator(
            task_id=f"quality_check_{city}",
            python_callable=_quality_check,
            op_kwargs={"city": city},
        )

        load = PythonOperator(
            task_id=f"load_weather_{city}",
            python_callable=_load_weather,
            op_kwargs={"city": city},
        )

        init_db >> extract >> transform >> quality_check >> quality_gate
        quality_gate >> load >> join_load

    quality_gate >> handle_quality_anomalies

    join_load >> log_ingestion_success >> end_pipeline
    handle_quality_anomalies >> end_pipeline
