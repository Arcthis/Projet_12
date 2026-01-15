# Data Engineering Project

## Structure du Projet

Le projet est séparé en deux parties principales :

1.  **Orchestration & Base de Données (Racine)** :
    *   Un fichier `docker-compose.yml` qui gère les conteneurs **PostgreSQL** (base de données) et **PgAdmin** (interface d'administration).

2.  **Pipeline ETL (`ETL_Python`)** :
    *   Un dossier dédié contenant le pipeline ETL dockerisé, incluant les scripts Python et leur propre configuration Docker.

## Sécurité

> **Fichier .env manquant :** Pour des raisons de sécurité, le fichier `.env` nécessaire au bon fonctionnement du pipeline **NE figure PAS** dans ce dépôt. Il contient des informations sensibles (token Slack, clés API, etc.) et ne doit jamais être commité. Assurez-vous de créer ce fichier localement avec les variables d'environnement requises avant de lancer le projet.
