# data_ingestion.py
# Description : Ce module gère l'ingestion des données en temps réel des stations de vélos des villes de Paris, Toulouse et Nantes,
# ainsi que les données descriptives des communes françaises à partir d'APIs publiques.

import os
from datetime import datetime
import requests

def get_paris_realtime_bicycle_data():
    """Récupère les données en temps réel des vélos disponibles à Paris et les enregistre localement."""
    
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "paris_realtime_bicycle_data.json")
    
def get_toulouse_realtime_bicycle_data():
    """Récupère les données en temps réel des vélos disponibles à Toulouse et les enregistre localement."""
    
    url = "https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/api-velo-toulouse-temps-reel/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "toulouse_realtime_bicycle_data.json")

def get_nantes_realtime_bicycle_data():
    """Récupère les données en temps réel des vélos disponibles à Nantes et les enregistre localement."""
    
    url = "https://data.nantesmetropole.fr/api/explore/v2.1/catalog/datasets/244400404_stations-velos-libre-service-nantes-metropole-disponibilites/exports/json"
    response = requests.request("GET", url)
    serialize_data(response.text, "nantes_realtime_bicycle_data.json")

def serialize_data(raw_json: str, file_name: str):
    """Sérialise les données JSON récupérées en un fichier local daté pour la traçabilité."""
    
    today_date = datetime.now().strftime("%Y-%m-%d")
    if not os.path.exists(f"data/raw_data/{today_date}"):
        os.makedirs(f"data/raw_data/{today_date}")
    with open(f"data/raw_data/{today_date}/{file_name}", "w") as fd:
        fd.write(raw_json)

def get_communes_data():
    """Récupère les données descriptives des communes françaises et les enregistre localement."""
    
    url = "https://geo.api.gouv.fr/communes?fields=nom,code&format=json&geometry=centre"
    response = requests.request("GET", url)
    if response.status_code == 200:
        serialize_data(response.text, "communes_data.json")
    else:
        print(f"Erreur lors de l'appel à l'API : {response.status_code}")
