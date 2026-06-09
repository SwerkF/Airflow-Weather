# Airflow Weather Pipeline

Environnement Airflow local avec Docker Compose.

## Prérequis

- Docker
- Docker Compose

## Installation et lancement

**1. Initialiser et démarrer Airflow**
```bash
docker compose up airflow-init
docker compose up -d
```

**2. Créer les tables PostgreSQL** (à faire une seule fois)
```bash
docker compose exec -T postgres psql -U airflow -d airflow < scripts/init.sql
```

**3. Vérifier que tout tourne**
```bash
docker compose ps
```

**Arrêter l'environnement**
```bash
docker compose down
```

> La connexion PostgreSQL (`postgres_weather`) est configurée automatiquement via le `.env`, pas besoin de la créer à la main.

## Accès

- **UI** : http://localhost:8080
- **Login** : `airflow` / `airflow`

## DAGs disponibles

| DAG | Description | Schedule |
|-----|-------------|----------|
| `weather_pipeline` | Fetch la météo de Paris, Tokyo et New York via Open-Meteo | `@daily` |

## Commandes utiles

```bash
# Lister les DAGs
docker compose exec airflow-apiserver airflow dags list

# Déclencher un DAG manuellement
docker compose exec airflow-apiserver airflow dags trigger weather_pipeline

# Déclencher avec des villes spécifiques
docker compose exec airflow-apiserver airflow dags trigger weather_pipeline --conf '{"cities": ["paris", "tokyo"]}'

# Vérifier les données insérées
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM weather_data;"
docker compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM ingestion_log;"

# Voir les logs d'un service
docker compose logs -f airflow-scheduler
```

## Résultats / Réponses aux question

Les résultats sont disponbiles dans /images.

### Analyse des logs.

![Logs Fetch Weather](images/logs_fetch_weather.png)

Ici, on voit que la météo de Tokyo a été récupérée avec succès. 22 Degrés Celsius et Partly Cloudy.

![Logs Save Weather](images/logs_save_weather.png)

Ici, on voit que la météo a été traitée avec succès. Short et t-shirt et que les données ont été sauvegardées avec succès.

### Explication des tâches

Pour ce TP j'ai utilisé l'API [Open-Meteo](https://open-meteo.com/) qui est gratuite et ne nécessite pas de clé API. Les 3 villes choisies sont Paris, Tokyo et New York. Les tâches sont générées automatiquement pour chaque ville via une boucle sur le dictionnaire `CITIES`.

![DAG Weather Pipeline](images/airflow_dag_tp3.png)

- **`start_pipeline`**: tâche vide qui sert de point de départ. Ça permet d'avoir un DAG propre dans l'UI plutôt que 3 tâches qui partent de nulle part.

- **`fetch_weather_{city}`**: récupère le JSON brut de l'API et le passe à la tâche suivante via XCom. Je ne touche pas aux données ici, c'est juste le fetch. Comme ça si l'API plante, je peux relancer uniquement cette tâche.

- **`process_weather_{city}`**: extrait les champs qui m'intéressent (`temperature_c`, `humidity_pct`, `wind_speed_kmh`, `weather_code`, `fetched_at`) et vérifie qu'ils sont tous là. Si un champ est manquant la tâche échoue, ce qui évite de sauvegarder des données incomplètes.

- **`load_weather_{city}`**: insère les données dans la table `weather_data` de PostgreSQL via `PostgresHook`. Une ligne par ville par exécution.

- **`check_pipeline`**: regarde si toutes les villes ont bien été chargées et redirige vers le bon résultat.

- **`log_ingestion`**: écrit une ligne dans la table `ingestion_log` avec le run ID, le nombre de villes et le statut `success`. Permet de tracer chaque exécution du pipeline.

- **`alert_failures`**: même chose mais avec le statut `partial_failure`, et log les villes qui ont échoué. Dans un vrai projet cette tâche enverrait une alerte par email ou Slack.