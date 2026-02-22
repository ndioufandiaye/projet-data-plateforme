# Data Platform 🚀

## Stack
- Docker
- Spark
- MinIO
- Python
- Jupyter / Marimo

## Setup

```bash
git clone ...
cd data-platform-projetISI
pip install -r requirements.txt
docker-compose up

## création et activation env virtuel

python -m venv venv
venv\Scripts\activate     # Windows

## installation des dépendances 

pip install -r requirements.txt

## pour dire à docker de recupéré tout les dossiers et fichiers de mon projet pour le mettre dans son conteneur
volumes:
  - ./:/app

##pour installer les dépendances directement sur le conteneur
docker exec -it marimo pip install -r requirements.txt

##pour aacéder dans le bash marimo
docker exec -it marimo bash

##pour nettoyé completement docker 
docker system prune -a

##cette commande permet de lancé la commande python dans le container marimo afin de lancer le script loadExcel.py
docker exec -it marimo python /app/pipelines/bronze/loadExcel.py

##pour recupéré le token de connexion à marimo
docker logs marimo