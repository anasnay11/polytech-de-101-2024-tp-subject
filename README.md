# Projet d'Analyse de Mobilité Urbaine

## Description
Ce projet implique la création d'un pipeline ETL pour l'ingestion, la transformation, et le stockage des données relatives à la disponibilité des vélos en temps réel dans plusieurs grandes villes de France, notamment Paris, Toulouse, et Nantes. L'objectif est de mettre en pratique les connaissances acquises en ingénierie de données en développant un système capable de traiter et analyser des données en temps réel.

## Fonctionnalités
- **Ingestion de données**: Extraction automatique des données depuis les APIs ouvertes des villes concernées.
- **Consolidation de données**: Normalisation et unification des données pour préparer des analyses.
- **Agrégation de données**: Synthèse des données pour générer des insights utiles et des rapports.

## Prérequis
- Python 3.8 ou supérieur
- Bibliothèques Python : Pandas, Requests, DuckDB
- Accès Internet pour la récupération des données des APIs

## Installation
Clonez le dépôt GitHub et installez les dépendances nécessaires :

bash
git clone https://github.com/votre_username/votre_repository.git
cd votre_repository
pip install pandas requests duckdb

## Structure du Projet
- data/: Contient les données brutes et transformées.
- src/: Contient les codes Python pour chaque étape du pipeline ETL.
- data/raw_data/: Dossier pour les données extraites des APIs.
- data/duckdb/: Dossier pour la base de données DuckDB utilisée pour le stockage des données.

## Usage
Exécutez le script principal pour lancer le pipeline de données :

bash
python main.py

## Documentation
Chaque module et fonction dans le projet est documenté pour expliquer son rôle et son fonctionnement. Des commentaires détaillés sont inclus pour faciliter la compréhension et la maintenance du code.

## Licence
Distribué sous la licence MIT. Voir LICENSE pour plus d'informations.

## Contact
NAY Anas - anas.nay@polytech-lille.net
DAHMANI Jibril - jibril.dahmani@polytech-lille.net
