import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)

TEMP_MIN = -80.0
TEMP_MAX = 60.0
HUMIDITY_MIN = 0
HUMIDITY_MAX = 100
WIND_MAX = 500.0
MAX_DATA_AGE_HOURS = 48


class QualityError(Exception):
    def __init__(self, check_name: str, detail: str):
        self.check_name = check_name
        self.detail = detail
        super().__init__(f"[{check_name}] {detail}")


def _check_completeness(city: str, data: dict) -> None:
    required = ["city", "fetched_at", "temperature_c", "humidity_pct", "wind_speed_kmh", "weather_code"]
    missing = [f for f in required if data.get(f) is None]
    if missing:
        raise QualityError("completeness", f"[{city}] Champs nuls ou absents : {missing}")


def _check_coherence(city: str, data: dict) -> None:
    temp = data["temperature_c"]
    hum = data["humidity_pct"]
    wind = data["wind_speed_kmh"]

    if not (TEMP_MIN <= temp <= TEMP_MAX):
        raise QualityError(
            "coherence",
            f"[{city}] Température hors plage : {temp} °C (attendu : {TEMP_MIN}–{TEMP_MAX})",
        )

    if not (HUMIDITY_MIN <= hum <= HUMIDITY_MAX):
        raise QualityError(
            "coherence",
            f"[{city}] Humidité hors plage : {hum} % (attendu : 0–100)",
        )

    if wind < 0 or wind > WIND_MAX:
        raise QualityError(
            "coherence",
            f"[{city}] Vitesse du vent hors plage : {wind} km/h",
        )


def _check_freshness(city: str, data: dict) -> None:
    fetched_at_str = data["fetched_at"]
    try:
        fetched_at = datetime.fromisoformat(fetched_at_str)
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)
        age = datetime.now(timezone.utc) - fetched_at
        if age > timedelta(hours=MAX_DATA_AGE_HOURS):
            raise QualityError(
                "freshness",
                f"[{city}] Donnée trop ancienne : {age.total_seconds() / 3600:.1f} h (max : {MAX_DATA_AGE_HOURS} h)",
            )
    except (ValueError, TypeError) as exc:
        raise QualityError("freshness", f"[{city}] Format de date invalide : {fetched_at_str} — {exc}")


CHECKS = [_check_completeness, _check_coherence, _check_freshness]

def run_quality_checks(city: str, data: dict) -> list[dict]:
    anomalies = []
    for check in CHECKS:
        try:
            check(city, data)
        except QualityError as exc:
            logger.warning(f"Check '{exc.check_name}' échoué : {exc.detail}")
            anomalies.append({"check_name": exc.check_name, "detail": exc.detail})
    return anomalies
