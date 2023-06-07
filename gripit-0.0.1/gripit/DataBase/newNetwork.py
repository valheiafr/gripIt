# -*- coding: utf-8 -*-
"""
Created on Wed May 31 14:57:55 2023

@author: valentin.pasche1
"""

def creatNetworkTable (network_numer, conn) :
    
    from gripit.DataBase import conStrPostgreSQL
    from sqlalchemy import create_engine
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import LineString
    
    """
    Crée un réseau avec les tronçons défini par les tables "liaisons" et "stations" de la base de données.
    Le réseau créé contient les géométries sous forme de "shapely.geometry.LineString",
        la longueur (à vol d'oiseau) de ces dérniers et l'identification des extrémités (coordonées et code).
        Lors de l'utilisation de la table "reseau", ajouter un facteur fractal.

    Parameters
    ----------
    network_numer : int / float
        Numero du nouveau réseau.
        Type "version" (ex. 4.01) suivant l'ampleur des modifications
    conn : dict
        Dictionnaire de connexion à la DB PostgreSQL.
        Contenant :
            - "hostname" # Adresse du serveur PostgreSQL
            - "database" # Nom de la base de données (une DB est à l'intérieur d'un serveur)
            - "username" # Nom d'utilisateur
            - "pwd" # Mot de passe de l'utilisateur
            - "port_id" # Port de communiquation avec le serveur (généralement 5432 avec PostgreSQL)
            - "schema" # Schéma de la DB qui contient le données et recoit la table créée

    Returns
    -------
    allLine : GeoDataFrame
        Réseau provenant des tables de la DB de même numéro.

    """
        
    ##### ##### #####

    """ Paramètres de connexion à la base de données """   
    connection = conn # Dictionaire de connexion à PostgreSQL
    uri = conStrPostgreSQL(connection) # Création de la chaine de caractères de connexion à la DB
    engine = create_engine(uri) # "engine" de connexion à la DB (connexion via sqlalchemy)
    
    ##### ##### #####
    
    """ Importation Données Implémentation - Stations (EPSG:2056) """
    tableName = f'stations_{network_numer}'
    sql_query = f'''SELECT * FROM "{connection['schema']}"."{tableName}"'''
    DB_stations = gpd.read_postgis(sql_query, engine, crs='EPSG:2056') # Importation des données "stations" depuis la DB
    DB_stations = DB_stations.astype({'lat':'float64', 'lng':'float64'}) # Réasignation des types de données pour éviter les erreures
    engine.dispose() # Important pour décharger la base de données, autrement la DB écoute en indéfiniment la console
    
    """ Extraction des coordonées EPSG:2056 des Points pour remplacer les colonnes 'lat et 'lng', nécessaire pour extraire la distance """
    DB_stations['lng'] = DB_stations.geom.x # Extraction de la longitude des points
    DB_stations['lat'] = DB_stations.geom.y # Extraction de la latitude des points
    
    """ Importation Donnée Gripit - Liaisons """
    tableName = f'liaisons_{network_numer}'
    sql_query = f'''SELECT * FROM "{connection['schema']}"."{tableName}"'''
    DB_liaisons = pd.read_sql_query(sql_query, engine) # Importation des données "liaisons" depuis la DB
    engine.dispose() # Important pour décharger la base de données, autrement la DB écoute en indéfiniment la console
    del tableName; del sql_query
    
    ##### ##### #####
    
    """ Création des lignes - Lower """
    lowerLine = DB_liaisons[["lower_a", "lower_b"]].rename(columns={'lower_a':'a','lower_b':'b'}) # Report des codes et changement des noms de colonne
    lowerLine = lowerLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='a', right_on='code') # Combine les DataFrame (similaire à SQL join)
    lowerLine = lowerLine.rename(columns={'lng':'lng_a','lat':'lat_a'}).drop(columns='code') # Corrections mise en forme
    lowerLine = lowerLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='b', right_on='code') # Combine les DataFrame (similaire à SQL join)
    lowerLine = lowerLine.rename(columns={'lng':'lng_b','lat':'lat_b'}).drop(columns='code') # Corrections mise en forme
    lowerLine['level'] = 1 # Lignes "lower", niveau 1, basse vitesse
    
    """ Création des lignes - Main """
    mainLine = DB_liaisons[["main_a", "main_b"]].rename(columns={'main_a':'a','main_b':'b'}) # Report des codes et changement des noms de colonne
    mainLine = mainLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='a', right_on='code') # Combine les DataFrame (similaire à SQL join)
    mainLine = mainLine.rename(columns={'lng':'lng_a','lat':'lat_a'}).drop(columns='code') # Corrections mise en forme
    mainLine = mainLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='b', right_on='code') # Combine les DataFrame (similaire à SQL join)
    mainLine = mainLine.rename(columns={'lng':'lng_b','lat':'lat_b'}).drop(columns='code') # Corrections mise en forme
    mainLine['level'] = 2 # Lignes "main", niveau 2, moyenne vitesse
    
    """ Création des lignes - Higher """
    higherLine = DB_liaisons[["higher_a", "higher_b"]].rename(columns={'higher_a':'a','higher_b':'b'}) # Report des codes et changement des noms de colonne
    higherLine = higherLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='a', right_on='code') # Combine les DataFrame (similaire à SQL join)
    higherLine = higherLine.rename(columns={'lng':'lng_a','lat':'lat_a'}).drop(columns='code') # Corrections mise en forme
    higherLine = higherLine.merge(DB_stations[["code", "lng", "lat"]], how='inner', left_on='b', right_on='code') # Combine les DataFrame (similaire à SQL join)
    higherLine = higherLine.rename(columns={'lng':'lng_b','lat':'lat_b'}).drop(columns='code') # Corrections mise en forme
    higherLine['level'] = 3 # Lignes "higher", niveau 3, haute vitesse
    
    """ GeoDataFrame complet """
    allLine = pd.concat([lowerLine, mainLine, higherLine], ignore_index=True) # Concaténer les 3 niveaux de lignes
    allLine['line'] = list(zip(list(zip(allLine.lng_a, allLine.lat_a)), list(zip(allLine.lng_b, allLine.lat_b)))) # Mise en forme des coordonées pour la fonction "LineString"
    allLine['geom'] = allLine.line.apply(lambda x : LineString(x)) # Crée le géométrie "ligne" des tronçons
    allLine = allLine.drop(columns='line') # Supprime la colonne inutile
    allLine = gpd.GeoDataFrame(allLine, geometry='geom', crs="EPSG:2056") # Tranforme le DataFrame en GeoDataFrame
    allLine['length'] = round(allLine.geom.length).astype('int32') # Extrait la longueur des tronçons [m] (longueur à vol d'oiseau, pas de facteur fractal)
    del lowerLine; del mainLine; del higherLine
    
    """ Mise en forme finale du GeoDataFRame, pour exportation vers la base de données """
    allLine = allLine.reindex(columns=['a','b','geom','level','length','lng_a','lat_a','lng_b','lat_b']).sort_values(by=['level','a','b']).reset_index(drop=True)
    
    return allLine



def creatStationsTable (file_path) :
    
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point
    
    """
    Crée une table avec les stations répértoriées par le fichier csv : "stations.csv".
    Le table créée contient les géométries des stations sous forme de "shapely.geometry.Point" (EPSG:2056),
        selon les colonnes (coordonées "lng" "lat" et l'identification "code").
                            
    Format du CSV : Séparateur de colonne = ';'
                    Séparateur de décimal = '.'
                    Entête (nom de colonne) = 1ère ligne
                    Latitudes et longitudes (dans le csv) en coordonnées polaire, WGS 84 (EPSG:4326)

    Parameters
    ----------
    file_path : "str"
        Chemin d'accès complet du fichier ".../stations.csv".
        (relatif ou absolut)

    Returns
    -------
    tablePoint : geopandas.GeoDataFrame
        Liste des stations provenant du csv "stations" de même numéro.

    """
        
    ##### ##### #####

    """ Importation Données Stations - Coordonnée: WGS 84 (EPSG:4326) """
    data = pd.read_csv(file_path, sep=';', decimal='.', header=0, dtype={
        'CODE':'str','NAME':'str','LAT':'float64','LNG':'float64'}) # Importation des données "stations" depuis le csv

    """ Création de la géométrie "Point" """
    data['coord'] = list(zip(data.LNG, data.LAT)) # Mise en forme des coordonées pour la fonction "Point"
    data['geom'] = data.coord.apply(lambda x : Point(x)) # Crée le géométrie "Point" des stations
    data = data.drop(columns='coord') # Supprime la colonne inutile

    """ Transformation des données en GeoDataFrame """
    data = gpd.GeoDataFrame(data, geometry='geom', crs="EPSG:4326").to_crs(epsg=2056) # Création GeoDataFrame et changement de système de coordonnées
    
    """ Mise en forme finale de la table """
    tablePoint = data.rename(columns={'CODE':'code','NAME':'name',
                'LNG':'lng','LAT':'lat'}).reindex(columns=['code','geom','name','lng','lat']).sort_values(by=['code']).reset_index(drop=True)
    
    return tablePoint



def creatLinksTable (file_path) :
    
    import pandas as pd
    
    """
    Crée une table avec les liaisons réértoriées par le fichier csv : "liaisons.csv".
    Le table créée répértorie l'associations les codes de stations des 3 niveaux de liaisons: lower (1), main (2) et higher (3).
                            
    Format du CSV : Séparateur de colonne = ';'
                    Entête (nom de colonne) = 1ère ligne

    Parameters
    ----------
    file_path : "str"
        Chemin d'accès complet du fichier ".../liaisons.csv".
        (relatif ou absolut)

    Returns
    -------
    tableLinks : pandas.DataFrame
        Liste des liaisons provenant du csv "liaisons" de même numéro.

    """
        
    ##### ##### #####

    """ Importation Données Liaisons """
    data = pd.read_csv(file_path, sep=';', header=0, dtype={
        'LOWER_A':'str','LOWER_B':'str','MAIN_A':'str','MAIN_B':'str','HIGHER_A':'str','HIGHER_B':'str'}) # Importation des données "laisons" depuis le csv
    
    """ Triage des colonnes """
    dataLower =data[['LOWER_A','LOWER_B']].sort_values(by=['LOWER_A','LOWER_B']).reset_index(drop=True) # Tri des liaisons (alphabétique)
    dataMain =data[['MAIN_A','MAIN_B']].sort_values(by=['MAIN_A','MAIN_B']).reset_index(drop=True) # Tri des liaisons (alphabétique)
    dataHigher =data[['HIGHER_A','HIGHER_B']].sort_values(by=['HIGHER_A','HIGHER_B']).reset_index(drop=True) # Tri des liaisons (alphabétique)
    
    data = pd.concat([dataLower, dataMain, dataHigher], axis=1) # Recombinaison après triage
    del dataLower; del dataMain; del dataHigher
    
    """ Mise en forme finale de la table """
    tableLinks = data.rename(columns={'LOWER_A':'lower_a','LOWER_B':'lower_b','MAIN_A':'main_a',
                                            'MAIN_B':'main_b','HIGHER_A':'higher_a','HIGHER_B':'higher_b'}
                                   ).reindex(columns=['lower_a','lower_b','main_a','main_b','higher_a','higher_b'])
    
    return tableLinks

