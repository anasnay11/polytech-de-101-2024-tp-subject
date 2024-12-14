# data_agregation.py
# Description : Ce module gère l'agrégation des données consolidées des stations de vélos pour Paris, Toulouse et Nantes,
# ainsi que les données descriptives des villes, en utilisant une base de données DuckDB pour créer des vues dimensionnelles et factuelles.

import duckdb

def create_agregate_tables():
    """Crée les tables d'agrégation nécessaires en utilisant les définitions SQL stockées."""
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    with open("data/sql_statements/create_agregate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def agregate_dim_station():
    """Agrège les données pour créer une dimension des stations dans la base de données analytique."""
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_STATION
    SELECT 
        ID,
        CODE,
        NAME,
        ADDRESS,
        LONGITUDE,
        LATITUDE,
        STATUS,
        CAPACITTY
    FROM CONSOLIDATE_STATION
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION);
    """

    con.execute(sql_statement)

def agregate_dim_city():
    """Agrège les données pour créer une dimension des villes dans la base de données analytique."""
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    sql_statement = """
    INSERT OR REPLACE INTO DIM_CITY
    SELECT 
        ID,
        NAME,
        NB_INHABITANTS
    FROM CONSOLIDATE_CITY
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """

    con.execute(sql_statement)

def agregate_fact_station_statements():
    """Agrège les données pour créer une table de faits des relevés des stations dans la base de données analytique."""
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)
    
    sql_statement = """
    INSERT OR REPLACE INTO FACT_STATION_STATEMENT
    SELECT STATION_ID, cc.ID as CITY_ID, BICYCLE_DOCKS_AVAILABLE, BICYCLE_AVAILABLE, LAST_STATEMENT_DATE, current_date as CREATED_DATE
    FROM CONSOLIDATE_STATION_STATEMENT
    JOIN CONSOLIDATE_STATION ON CONSOLIDATE_STATION.ID = CONSOLIDATE_STATION_STATEMENT.STATION_ID
    LEFT JOIN CONSOLIDATE_CITY as cc ON cc.ID = CONSOLIDATE_STATION.CITY_CODE
    WHERE CITY_CODE != 0 
        AND CONSOLIDATE_STATION_STATEMENT.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION_STATEMENT)
        AND CONSOLIDATE_STATION.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
        AND cc.CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_CITY);
    """
    
    con.execute(sql_statement)
