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
| `weather_pipeline` | Fetch la météo de Tokyo et sauvegarde en JSON | `@daily` |

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
`fetch_weather`: Appelle l'API wttr.in pour récupérer la météo de Tokyo en temps réel.

`process_weather`: Analyse la température et génère un conseil vestimentaire selon des seuils définis (< 10°C, < 20°C, ou plus).

`save_weather`: Sérialise les données traitées en JSON et les sauvegarde dans /tmp/ avec la date du jour comme nom de fichier.