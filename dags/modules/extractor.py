import logging
import requests

logger = logging.getLogger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_city_weather(city: str, lat: float, lon: float) -> dict:
    logger.info(f"[{city}] Appel Open-Meteo — lat={lat}, lon={lon}")
    response = requests.get(
        OPEN_METEO_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        },
        timeout=15,
    )
    
    response.raise_for_status()
    data = response.json()
    logger.info(f"[{city}] Réponse reçue")
    return data
