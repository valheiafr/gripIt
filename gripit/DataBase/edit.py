# -*- coding: utf-8 -*-
"""
Created on Wed May 31 14:57:55 2023

@author: valentin.pasche1
"""

def setTable_notNULL_PK (table, dict_conn, list_columns):
    
    import psycopg2
    
    """
    Fonction qui modifie les colonnes de la base de données en not null et en primary key.
    Si plusieurs colonnes sont définies PK, l'ascossiation des colonnes forme "une" PK.

    Parameters
    ----------
    table : str
        Nom de la table à modifier.
    dict_conn : dict
        Dictionnaire de connexion
        !! Avec schéma !!.
    list_columns : list of str ['col1','col2',...]
        Liste des colonnes à définir comme primary key et non nulle.

    Returns
    -------
    None.

    """
 
    hostname = dict_conn["hostname"]
    database = dict_conn["database"]
    username = dict_conn["username"]
    pwd = dict_conn["pwd"]
    port_id = dict_conn["port_id"]
    conn = dict_conn["conn"]
    schema = dict_conn["schema"]
    
    try:
        with psycopg2.connect(
                                host = hostname,
                                dbname = database,
                                user = username,
                                password = pwd,
                                port = port_id) as conn:
            
            
            with conn.cursor() as cur:
                script_not_null_all = ""
                for column in list_columns:
                    script_not_null_one = f'''
ALTER TABLE IF EXISTS "{schema}"."{table}"
    ALTER COLUMN "{column}" SET NOT NULL;
                    '''
                    script_not_null_all = script_not_null_all + script_not_null_one
                
                string="("
                for column in list_columns:
                    string = string + f'''"{column}", '''
                string = string[:-2] + ")"
         
                script_pk = f'''
ALTER TABLE IF EXISTS "{schema}"."{table}"
    ADD PRIMARY KEY {string};
                '''
                script = script_not_null_all + script_pk
                cur.execute(script)
               
    except Exception as error:
        print(error)
        
    finally:
        if conn is not None:
            conn.close()


##### ##### #####

def tableToPostgreSQL (data, table, network_numer, conn, if_exists='fail') :
    
    from sqlalchemy import create_engine
    from sqlalchemy.dialects.postgresql import (DOUBLE_PRECISION,INTEGER,SMALLINT,VARCHAR)
    from gripit.DataBase import conStrPostgreSQL
    
    """
    Importe les stations crée par la fonction createSationsTable du module "newNetwork".
    ou
    Importe les liaisons crée par la fonction createLinksTable du module "newNetwork".
    ou
    Importe le réseau crée par la fonction "createNetworkTable" du module "newNetwork".


    Parameters
    ----------
    data : geopandas.GeoDataFrame ou pandas.DataFrame
        GeoDataFrame : "stations ou "reseau""
        DataFrame : "liaisons"
    
    table : str
        Type de la table.
        "Stations", "liaisons" ou "reseau".
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
    if_exists : "str", optional
        Condition de réécriture dans la DB. The default is "fail".
            - "fail" : Ne réécrit pas sur une table de même nom déjà existante
            - "replace" : Opération irréversible. Réécrit sur une table de même nom déjà existante

    Returns
    -------
    "table"_"network_numer" : SQL table (dans DB)
        Table "stations", table "liaisons" provenant du fichier brut csv.
        ou
        Table "reseau" provenant des tables stations et liaisons de la DB de même numéro.

    """

    ##### ##### #####        

    """ Paramètres de connexion à la base de données """   
    connection = conn # Dictionaire de connexion à PostgreSQL
    uri = conStrPostgreSQL(connection) # Création de la chaine de caractères de connexion à la DB
    engine = create_engine(uri) # "engine" de connexion à la DB (connexion via sqlalchemy)
    
    ##### ##### #####

    if table == 'stations' :
        
        tableName = f'{table}_{network_numer}'
        data.to_postgis(tableName, engine, schema=connection['schema'], if_exists=if_exists, index=False,
                        dtype={'code':VARCHAR,'name':VARCHAR,'lng':DOUBLE_PRECISION,'lat':DOUBLE_PRECISION}) # if_exists='replace' si besoin, variable en haut du script
        engine.dispose() # Important pour décharger la base de données, autrement la DB écoute en indéfiniment la console
        setTable_notNULL_PK(tableName, connection, ['code']) # Définit les clés primaires de la table (connexion via psycopg2, "engine" non nécessaire)
    
    
    elif table == 'liaisons' :
        
        tableName = f'{table}_{network_numer}'
        data.to_sql(tableName, engine, schema=connection['schema'], if_exists=if_exists, index=False, dtype={'lower_a':VARCHAR,
        'lower_b':VARCHAR,'main_a':VARCHAR,'main_b':VARCHAR,'higher_a':VARCHAR,'higher_b':VARCHAR}) # if_exists='replace' si besoin, variable en haut du script
        engine.dispose() # Important pour décharger la base de données, autrement la DB écoute en indéfiniment la console
    
    elif table == 'reseau' : 
        
        tableName = f'{table}_{network_numer}'
        data.to_postgis(tableName, engine, schema=connection['schema'], if_exists=if_exists, index=False,
                        dtype={'a':VARCHAR,'b':VARCHAR,'level':SMALLINT,'length':INTEGER,'lng_a':DOUBLE_PRECISION,
                               'lat_a':DOUBLE_PRECISION,'lng_b':DOUBLE_PRECISION,'lat_b':DOUBLE_PRECISION}) # if_exists='replace' si besoin, variable en haut du script
        engine.dispose() # Important pour décharger la base de données, autrement la DB écoute en indéfiniment la console
        setTable_notNULL_PK(tableName, connection, ['a','b']) # Définit les clés primaires de la table (connexion via psycopg2, "engine" non nécessaire)
    
    else : None


##### ##### #####

def scriptSQL (dict_conn, script):
    import psycopg2

    """
    Fonction qui execute un script SQL directement dans la base de données.

    Parameters
    ----------
    dict_conn : dict
        Dictionnaire de connexion
        !! Avec schéma !!.
    script : srcipt SQL à executer

    Returns
    -------
    None.

    """

    hostname = dict_conn["hostname"]
    database = dict_conn["database"]
    username = dict_conn["username"]
    pwd = dict_conn["pwd"]
    port_id = dict_conn["port_id"]
    conn = dict_conn["conn"]
    
    try:
        with psycopg2.connect(
                                host = hostname,
                                dbname = database,
                                user = username,
                                password = pwd,
                                port = port_id) as conn:
            with conn.cursor() as cur:
                cur.execute(script)
               
    except Exception as error:
        print(error)
        
    finally:
        if conn is not None:
            conn.close()

