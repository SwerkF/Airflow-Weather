from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import requests
import json
import logging
import os

logger = logging.getLogger(__name__)

CITIES = {
    "paris": {"lat": 48.8566, "lon": 2.3522},
    "tokyo": {"lat": 35.6762, "lon": 139.6503},
    "new_york": {"lat": 40.7128, "lon": -74.0060},
}

OUTPUT_DIR = "/tmp/weather_pipeline"


def fetch_weather(city, lat, lon, **context):
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


def check_pipeline(**context):
    failed = []
    for city in CITIES:
        row = context["ti"].xcom_pull(key=f"processed_{city}", task_ids=f"process_weather_{city}")
        if row is None:
            failed.append(city)

    if failed:
        logger.warning(f"Villes en échec : {failed}")
        return "alert_failures"
    return "log_execution_success"


def alert_failures(**context):
    failed = []
    for city in CITIES:
        row = context["ti"].xcom_pull(key=f"processed_{city}", task_ids=f"process_weather_{city}")
        if row is None:
            failed.append(city)
    logger.error(f"[FAILURE] Pipeline terminé avec des erreurs sur : {failed}")


def log_execution_success(**context):
    logger.info(f"[SUCCESS] {len(CITIES)} cities ingested -> {OUTPUT_DIR}")
    logger.info(f"Done. Fichier : {OUTPUT_DIR}/{datetime.now().strftime('%Y-%m-%d')}.json")


def save_weather(**context):
    try:
        all_data = []
        for city in CITIES:
            row = context["ti"].xcom_pull(key=f"processed_{city}", task_ids=f"process_weather_{city}")
            if row is None:
                logger.warning(f"[{city}] Données manquantes, ville ignorée")
                continue
            all_data.append(row)

        if not all_data:
            raise ValueError("Aucune donnée à sauvegarder")

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = f"{OUTPUT_DIR}/{datetime.now().strftime('%Y-%m-%d')}.json"

        with open(filename, "w") as f:
            json.dump(all_data, f, indent=2)

        logger.info(f"{len(all_data)} ville(s) sauvegardées dans {filename}")
        logger.info(json.dumps(all_data, indent=2))
    except Exception as e:
        logger.error(f"Erreur lors de la sauvegarde : {e}")
        raise


with DAG(
    dag_id="weather_pipeline",
    description="Fetch, traite et sauvegarde la météo de 3 villes via Open-Meteo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["météo", "open-meteo"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    save_task = PythonOperator(
        task_id="save_weather",
        python_callable=save_weather,
    )

    check = BranchPythonOperator(
        task_id="check_pipeline",
        python_callable=check_pipeline,
        trigger_rule="all_done",
    )

    alert = PythonOperator(
        task_id="alert_failures",
        python_callable=alert_failures,
    )

    success = PythonOperator(
        task_id="log_execution_success",
        python_callable=log_execution_success,
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

        start >> fetch_task >> process_task >> save_task

    save_task >> check >> [alert, success]
