# Data Engineering Project

## Contexte et objectifs

Ce projet s'inscrit dans un contexte RH fictif où une entreprise souhaite automatiser le traitement de ses données salariés afin de calculer automatiquement des primes liées à la mobilité durable et au bien-être au travail.

L'objectif est de concevoir et déployer un pipeline de données complet, de bout en bout, capable de :

- Centraliser les données RH et sportives dans une base de données PostgreSQL.
- Calculer automatiquement l'éligibilité des salariés à une **prime transport** (basée sur le mode de déplacement domicile-travail et la distance géocodée) et à des **jours de bien-être** (basés sur l'activité physique enregistrée).
- Simuler des activités sportives réalistes à la manière de l'application Strava.
- Notifier les parties prenantes en temps réel via Slack lors de l'enregistrement d'une nouvelle activité.
- Superviser l'ensemble du pipeline via un stack de monitoring dédié.

La démarche de réalisation a suivi une approche itérative : modélisation des besoins métiers, conception de l'architecture technique, développement du pipeline ETL, mise en place de la base de données et du monitoring, puis validation avec des données simulées.

---

## Technologies utilisées

### Orchestration et conteneurisation
- **Docker** et **Docker Compose** : chaque composant du projet tourne dans un conteneur isolé, ce qui garantit la portabilité et la reproductibilité de l'environnement.

### Base de données
- **PostgreSQL** : base de données relationnelle utilisée pour stocker les données brutes (schéma `raw`) et les données transformées (schéma `analytics`).
- **PgAdmin** : interface web d'administration de PostgreSQL.

### Pipeline ETL
- **Python** : langage principal du pipeline.
- **Pandas** : manipulation et transformation des données tabulaires.
- **SQLAlchemy** et **psycopg2** : connexion et interactions avec PostgreSQL.
- **Geopy** et **Nominatim** : géocodage des adresses (domicile et entreprise) pour calculer les distances réelles.
- **Mimesis** : génération de données fictives réalistes (noms, prénoms, etc.).
- **python-dotenv** : gestion des variables d'environnement via un fichier `.env`.

### Notifications
- **Slack SDK** : envoi de messages automatiques sur Slack lors de l'enregistrement d'une nouvelle activité sportive.

### Monitoring
- **Prometheus** : collecte des métriques exposées par le pipeline ETL (nombre de cycles, durée, lignes traitées, erreurs de géocodage, etc.).
- **Grafana** : visualisation des métriques sous forme de tableaux de bord.
- **Node Exporter** et **cAdvisor** : supervision de l'infrastructure (ressources système et conteneurs Docker).
- **Postgres Exporter** : exposition des métriques internes de PostgreSQL vers Prometheus.

### Visualisation des données
- **Power BI** : création d'un tableau de bord analytique à destination des équipes RH pour visualiser les résultats du pipeline.

---

## Structure du projet

Le projet est organisé en trois composants principaux :

1. **Orchestration & Base de données (racine)** : un fichier `docker-compose.yml` qui gère les conteneurs PostgreSQL et PgAdmin.

2. **Pipeline ETL (`ETL_Python`)** : le cœur du projet. Il contient les scripts Python organisés par étape (extraction, transformation, chargement, génération, notification, monitoring) ainsi que leur propre configuration Docker.

3. **Monitoring (`Monitoring`)** : un second `docker-compose` dédié qui lance Prometheus, Grafana, Node Exporter, cAdvisor et Postgres Exporter, tous connectés au réseau Docker commun.

---

## Installation et utilisation

### Prérequis
- Docker et Docker Compose installés sur la machine.
- Un fichier `.env` à la racine du projet (voir la section Sécurité ci-dessous).

### Lancement

1. **Démarrer la base de données** depuis la racine du projet :

```bash
docker-compose up -d
```

2. **Démarrer le pipeline ETL** depuis le dossier `ETL_Python` :

```bash
docker-compose -f docker-compose-etl.yml up -d
```

3. **Démarrer le monitoring** depuis le dossier `Monitoring` :

```bash
docker-compose -f docker-compose-monitoring.yml up -d
```

### Initialisation des données

Avant le premier cycle ETL de production, il est possible d'initialiser la base avec un historique simulé d'activités (1 000 activités sur 1 an) en exécutant le script `init_data.py` dans le conteneur ETL.

### Cycle de fonctionnement

Une fois lancé, le pipeline tourne en boucle toutes les 24 heures. À chaque cycle, il :
- Vérifie l'intégrité du pipeline via une suite de tests automatiques.
- Extrait les données RH et sportives depuis PostgreSQL.
- Génère de nouvelles activités sportives simulées.
- Calcule les distances domicile-travail par géocodage et détermine l'éligibilité à la prime transport.
- Détermine l'éligibilité aux jours de bien-être en fonction du nombre d'activités enregistrées sur les 12 derniers mois.
- Charge les résultats dans le schéma `analytics` de PostgreSQL.
- Envoie des notifications Slack pour chaque nouvelle activité détectée.

### Accès aux interfaces

| Interface | URL |
|---|---|
| PgAdmin | `http://localhost:<PGADMIN_PORT>` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |
| Métriques ETL | `http://localhost:8050` |

### Sécurité

> **Fichier .env manquant :** Pour des raisons de sécurité, le fichier `.env` nécessaire au bon fonctionnement du pipeline **NE figure PAS** dans ce dépôt. Il contient des informations sensibles (token Slack, clés API, identifiants de base de données, etc.) et ne doit jamais être commité. Assurez-vous de créer ce fichier localement avec les variables d'environnement requises avant de lancer le projet.

---

## Résultats obtenus

Le pipeline produit, à chaque cycle, les données suivantes dans le schéma `analytics` de PostgreSQL :

- **Prime transport** : pour chaque salarié, le pipeline calcule la distance réelle entre son domicile et son lieu de travail via géocodage, vérifie son mode de déplacement, et applique une prime de **5 % du salaire brut** aux salariés éligibles (déplacement en vélo, trottinette, marche ou running, dans la limite des distances maximales définies).
- **Jours de bien-être** : les salariés ayant enregistré au moins 15 activités physiques sur les 12 derniers mois sont identifiés comme éligibles à des jours de bien-être supplémentaires.
- **Activités Strava** : les activités sportives simulées sont persistées en base et consultables dans le tableau de bord Power BI.

Les résultats sont visualisés dans un **tableau de bord Power BI** (disponible dans le dossier `Livrables`) qui offre une vue synthétique des données RH, des primes calculées et de l'activité sportive des salariés.

Côté supervision, le **tableau de bord Grafana** permet de suivre en temps réel la bonne exécution du pipeline : nombre de cycles réussis ou en erreur, durée de traitement, nombre de lignes traitées ou non éligibles, et taux d'échec de géocodage.
