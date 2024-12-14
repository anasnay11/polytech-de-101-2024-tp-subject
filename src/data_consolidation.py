import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_CODE = 1
TOULOUSE_CITY_CODE = 0
NANTES_CITY_CODE = 2

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_station_data():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}
    
    # Consolidation Paris
    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["address"] = None
    paris_raw_data_df["created_date"] = date.today()

    paris_station_data_df = paris_raw_data_df[[
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]].copy()

    paris_station_data_df.rename(columns={
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")
    
    
    
    # Consolidation Toulouse
    data = {}
    
    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    #toulouse_raw_data_df["address"] = None
    toulouse_raw_data_df["created_date"] = date.today()
    
    # Récupérer l'ID de Toulouse depuis la table CONSOLIDATE_CITY
    city_code_query = con.execute("SELECT id FROM CONSOLIDATE_CITY WHERE NAME = 'Toulouse' LIMIT 1;")
    toulouse_city_code = city_code_query.fetchall()
    
    # Assigner la valeur récupérée à chaque ligne
    if toulouse_city_code:
        toulouse_raw_data_df["code_insee_commune"] = toulouse_city_code[0][0]


    toulouse_station_data_df = toulouse_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "code_insee_commune",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]].copy()

    toulouse_station_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "contract_name": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")
    
    # Consolidation Nantes
    
    data = {}
    
    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    #nantes_raw_data_df["address"] = None
    nantes_raw_data_df["created_date"] = date.today()
    
    # Récupérer l'ID de Nantes depuis la table CONSOLIDATE_CITY
    city_code_query = con.execute("SELECT id FROM CONSOLIDATE_CITY WHERE NAME = 'Nantes' LIMIT 1;")
    nantes_city_code = city_code_query.fetchall()
    
    # Assigner la valeur récupérée à chaque ligne
    if nantes_city_code:
        nantes_raw_data_df["code_insee_commune"] = nantes_city_code[0][0]


    nantes_station_data_df = nantes_raw_data_df[[
        "id",
        "number",
        "name",
        "contract_name",
        "code_insee_commune",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]].copy()

    nantes_station_data_df.rename(columns={
        "number": "code",
        "name": "name",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "status": "status",
        "contract_name": "city_name",
        "code_insee_commune": "city_code"
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")

today_date = datetime.now().strftime("%Y-%m-%d")

def consolidate_city_data():
    with duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False) as con:
        # Handle communes data
        with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
            communes_data = json.load(fd)
        
        # Normaliser les données des communes
        communes_df = pd.json_normalize(communes_data)
        
        # Ajout d'une colonne pour les habitants, ici en tant que placeholder
        communes_df["nb_inhabitants"] = None  # Placeholder for inhabitant data

        # Filtrage et renommage des colonnes
        communes_city_df = communes_df[['code', 'nom', 'nb_inhabitants']].copy()  # Assurez-vous que ces colonnes existent dans vos données
        communes_city_df.rename(columns={
            "code": "id",
            "nom": "name"
        }, inplace=True)
        
        # Suppression des doublons
        communes_city_df.drop_duplicates(inplace=True)

        # Ajout de la date de création
        communes_city_df["created_date"] = date.today()

        # Préparation des données pour l'insertion en bloc
        values = []
        for _, row in communes_city_df.iterrows():
            # Échapper les valeurs de texte pour éviter les erreurs SQL
            id_value = str(row['id']).replace("'", "''")  # Échapper les apostrophes
            name_value = str(row['name']).replace("'", "''")  # Échapper les apostrophes
            nb_inhabitants_value = row['nb_inhabitants']
            created_date_value = row['created_date']

            # Si nb_inhabitants est None, le remplacer par NULL en SQL
            if nb_inhabitants_value is None:
                nb_inhabitants_value = 'NULL'
            else:
                nb_inhabitants_value = str(nb_inhabitants_value)

            # Ajout des valeurs à la liste pour l'insertion en bloc
            values.append(f"('{id_value}', '{name_value}', {nb_inhabitants_value}, '{created_date_value}')")

        # Construction de la requête d'insertion en bloc
        insert_query = f"""
        INSERT INTO CONSOLIDATE_CITY (id, name, nb_inhabitants, created_date)
        VALUES {', '.join(values)};
        """
        
        # Exécution de la requête d'insertion en bloc
        con.execute(insert_query)



def consolidate_station_statement_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    
    
    # Paris
    
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    paris_raw_data_df = pd.json_normalize(data)
    paris_raw_data_df["station_id"] = paris_raw_data_df["stationcode"].apply(lambda x: f"{PARIS_CITY_CODE}-{x}")
    paris_raw_data_df["created_date"] = date.today()
    paris_station_statement_data_df = paris_raw_data_df[[
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"
    ]].copy()
    
    paris_station_statement_data_df.rename(columns={
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")
    
    
    # Toulouse
    
    data = {}

    with open(f"data/raw_data/{today_date}/toulouse_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    toulouse_raw_data_df = pd.json_normalize(data)
    toulouse_raw_data_df["station_id"] = toulouse_raw_data_df["number"].apply(lambda x: f"{TOULOUSE_CITY_CODE}-{x}")
    toulouse_raw_data_df["created_date"] = date.today()
    toulouse_station_statement_data_df = toulouse_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]].copy()
    
    toulouse_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")
    
    # Nantes
    
    data = {}

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    nantes_raw_data_df = pd.json_normalize(data)
    nantes_raw_data_df["station_id"] = nantes_raw_data_df["number"].apply(lambda x: f"{NANTES_CITY_CODE}-{x}")
    nantes_raw_data_df["created_date"] = date.today()
    nantes_station_statement_data_df = nantes_raw_data_df[[
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"
    ]].copy()
    
    nantes_station_statement_data_df.rename(columns={
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }, inplace=True)

    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")
