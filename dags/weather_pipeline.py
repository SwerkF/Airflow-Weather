from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

def fetch_weather(**context):
    response = requests.get(
        "https://wttr.in/Tokyo",
        params={"format": "j1"}
    )
    data = response.json()
    
    temp = data["current_condition"][0]["temp_C"]
    desc = data["current_condition"][0]["weatherDesc"][0]["value"]
    
    print(f"Tokyo : {temp}°C - {desc}")
    
    context["ti"].xcom_push(key="weather", value={"temp": temp, "desc": desc})

def process_weather(**context):
    weather = context["ti"].xcom_pull(key="weather", task_ids="fetch_weather")
    
    temp = int(weather["temp"])
    
    if temp < 10:
        conseil = "Prends un manteau"
    elif temp < 20:
        conseil = "Une veste suffit"
    else:
        conseil = "Short et t-shirt"
    
    result = {**weather, "conseil": conseil}
    print(f"Conseil du jour : {conseil}")
    
    context["ti"].xcom_push(key="processed", value=result)

def save_weather(**context):
    processed = context["ti"].xcom_pull(key="processed", task_ids="process_weather")
    
    filename = f"/tmp/weather_{datetime.now().strftime('%Y%m%d')}.json"
    
    with open(filename, "w") as f:
        json.dump(processed, f, indent=2)
    
    print(f"Données sauvegardées dans {filename}")
    print(json.dumps(processed, indent=2))

with DAG(
    dag_id="weather_pipeline",
    description="Fetch, traite et sauvegarde la météo de Tokyo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["météo", "tokyo"],
) as dag:
    t1 = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    t2 = PythonOperator(
        task_id="process_weather",
        python_callable=process_weather,
    )

    t3 = PythonOperator(
        task_id="save_weather",
        python_callable=save_weather,
    )

    t1 >> t2 >> t3