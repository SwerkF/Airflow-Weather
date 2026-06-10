import logging

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = [
    "time",
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "weather_code",
]

def transform_weather(city: str, raw_data: dict, force_anomaly: bool = False) -> dict:
    current = raw_data.get("current", {})

    missing = [f for f in REQUIRED_FIELDS if f not in current]
    if missing:
        raise ValueError(f"[{city}] Champs manquants dans la réponse API : {missing}")

    result = {
        "city": city,
        "fetched_at": current["time"],
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }

    if force_anomaly:
        logger.warning(f"[{city}] SIMULATION anomalie — température forcée à 150 °C")
        result["temperature_c"] = 150.0

    logger.info(
        f"[{city}] {result['temperature_c']} °C, "
        f"humidité {result['humidity_pct']} %, vent {result['wind_speed_kmh']} km/h"
    )
    return result
