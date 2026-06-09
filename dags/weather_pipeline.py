from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import requests
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = "postgres_weather"

CITIES = {
    "paris":    {"lat": 48.8566, "lon": 2.3522},
    "tokyo":    {"lat": 35.6762, "lon": 139.6503},
    "new_york": {"lat": 40.7128, "lon": -74.0060},
}


def fetch_weather(city, lat, lon, **context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    if city not in active_cities:
        logger.info(f"[{city}] Ignorée (non sélectionnée dans les params)")
        return

    try:
        logger.info(f"[{city}] Appel API Open-Meteo (lat={lat}, lon={lon})")
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        logger.info(f"[{city}] JSON brut récupéré avec succès")
        context["ti"].xcom_push(key=f"raw_{city}", value=data)
    except requests.exceptions.Timeout:
        logger.error(f"[{city}] Timeout lors de l'appel API")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"[{city}] Erreur HTTP : {e}")
        raise
    except Exception as e:
        logger.error(f"[{city}] Erreur inattendue : {e}")
        raise


def process_weather(city, **context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    if city not in active_cities:
        return

    try:
        raw = context["ti"].xcom_pull(key=f"raw_{city}", task_ids=f"fetch_weather_{city}")

        if raw is None:
            raise ValueError(f"[{city}] Aucune donnée brute trouvée dans XCom")

        current = raw.get("current", {})

        required_fields = ["time", "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "weather_code"]
        for field in required_fields:
            if field not in current:
                raise KeyError(f"[{city}] Champ manquant : {field}")

        result = {
            "city": city,
            "fetched_at": current["time"],
            "temperature_c": current["temperature_2m"],
            "humidity_pct": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
            "weather_code": current["weather_code"],
        }

        logger.info(f"[{city}] Traitement OK : {result['temperature_c']}°C, humidité {result['humidity_pct']}%, vent {result['wind_speed_kmh']} km/h")
        context["ti"].xcom_push(key=f"processed_{city}", value=result)
    except (KeyError, ValueError) as e:
        logger.error(f"[{city}] Erreur de traitement : {e}")
        raise
    except Exception as e:
        logger.error(f"[{city}] Erreur inattendue : {e}")
        raise


def load_weather(city, **context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    if city not in active_cities:
        return

    try:
        data = context["ti"].xcom_pull(key=f"processed_{city}", task_ids=f"process_weather_{city}")

        if data is None:
            raise ValueError(f"[{city}] Aucune donnée à charger")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(
            """
            INSERT INTO weather_data (city, fetched_at, temperature_c, humidity_pct, wind_speed_kmh, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(
                data["city"],
                data["fetched_at"],
                data["temperature_c"],
                data["humidity_pct"],
                data["wind_speed_kmh"],
                data["weather_code"],
            ),
        )

        logger.info(f"[{city}] Données insérées dans PostgreSQL")
        context["ti"].xcom_push(key=f"loaded_{city}", value=True)
    except Exception as e:
        logger.error(f"[{city}] Erreur lors du chargement PostgreSQL : {e}")
        raise


def check_pipeline(**context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    failed = [
        city for city in active_cities
        if not context["ti"].xcom_pull(key=f"loaded_{city}", task_ids=f"load_weather_{city}")
    ]

    if failed:
        logger.warning(f"Villes en échec : {failed}")
        return "alert_failures"
    return "log_ingestion"


def log_ingestion(**context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    dag_run_id = context["dag_run"].run_id

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        "INSERT INTO ingestion_log (dag_run_id, cities_count, status) VALUES (%s, %s, %s)",
        parameters=(dag_run_id, len(active_cities), "success"),
    )
    logger.info(f"[SUCCESS] {len(active_cities)} villes insérées. Run ID: {dag_run_id}")


def alert_failures(**context):
    active_cities = context["params"].get("cities", list(CITIES.keys()))
    failed = [
        city for city in active_cities
        if not context["ti"].xcom_pull(key=f"loaded_{city}", task_ids=f"load_weather_{city}")
    ]

    dag_run_id = context["dag_run"].run_id
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        "INSERT INTO ingestion_log (dag_run_id, cities_count, status) VALUES (%s, %s, %s)",
        parameters=(dag_run_id, len(active_cities) - len(failed), "partial_failure"),
    )
    logger.error(f"[FAILURE] Villes en échec : {failed}")


with DAG(
    dag_id="weather_pipeline",
    description="Pipeline complet Open-Meteo → transformation → PostgreSQL",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["météo", "open-meteo", "postgresql"],
    params={
        "cities": ["paris", "tokyo", "new_york"],
    },
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    check = BranchPythonOperator(
        task_id="check_pipeline",
        python_callable=check_pipeline,
        trigger_rule="all_done",
    )

    ingestion = PythonOperator(
        task_id="log_ingestion",
        python_callable=log_ingestion,
    )

    alert = PythonOperator(
        task_id="alert_failures",
        python_callable=alert_failures,
    )

    for city, coords in CITIES.items():
        fetch_task = PythonOperator(
            task_id=f"fetch_weather_{city}",
            python_callable=fetch_weather,
            op_kwargs={"city": city, "lat": coords["lat"], "lon": coords["lon"]},
        )

        process_task = PythonOperator(
            task_id=f"process_weather_{city}",
            python_callable=process_weather,
            op_kwargs={"city": city},
        )

        load_task = PythonOperator(
            task_id=f"load_weather_{city}",
            python_callable=load_weather,
            op_kwargs={"city": city},
        )

        start >> fetch_task >> process_task >> load_task >> check

    check >> [ingestion, alert]
