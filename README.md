# Airflow Weather Pipeline

Environnement Airflow local avec Docker Compose.

## Prérequis

- Docker
- Docker Compose

## Lancer l'environnement

```bash
# Initialiser airflow
docker compose up airflow-init

# Démarrer airflow
docker compose up -d

# Arrêter airflow
docker compose down
```

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

- **`save_weather`**: regroupe les données des 3 villes et les sauvegarde dans un fichier `/tmp/weather_pipeline/YYYY-MM-DD.json`. J'ai décidé de mettre un fichier pour plus de simplicité mais j'aurais pu mettre 3 fichiers différents pour chaque ville et classer par dossier (date/nom_ville).

- **`check_pipeline`**: regarde si toutes les villes ont bien été traitées et redirige vers le bon résultat.

- **`log_execution_success`**: log un message de succès avec le chemin du fichier produit. Permet de tracer les exécutions dans les logs Airflow.

- **`alert_failures`**: log les villes qui ont échoué. Dans un vrai projet cette tâche enverrait une alerte par email ou Slack.