# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 11:02:21 2023

@author: valentin.pasche1
"""

def chargeTJM_onePoint (coordinates: tuple, ri: int, re: int, conn: dict, trafic:str='all', tim_taux:float=1.6):    
    """
    Calcule la charge de trafic TJM entrent dans une zone circulaire de rayon défini, en nombre de voyageurs.
    La zone d'exclusion, suppression des trajets courts, est un buffer de la zone initale,
        la zone d'exclusion est supprimée/obselette si les 2 rayons ont la même valeur.
    TIM => Taux journalier moyen

    Parameters
    ----------
    coordinates : tuple(int,int)
        Coordonnées géographique du centre de la zone.
        Coordonnées - EPSG:2056 (CH1903+ / LV95)
        Format (x,y) ou (lng,lat)
        
    ri : int
        [km] Rayon de la zone circulaire initale.
        
    re : int
        [km] Rayon de la zone circulaire d'exclusion.
        
    conn : dict
        Dictionaire de connection à la base de données,
        Schéma 'MNTP' dans le code source pour les valeurs de flux.
        Table 'fulxCH' dans le code source.
        
    trafic : str, optional
        trafic='all'
            Calcule la somme du trafic, total voiture et transport public.
        trafic='separate'
            Calcule le trafic total, voiture et transport public.
        The default is 'all'.

    tim_taux : float, optional
        Modifie le taux de remplissage des transport en voiture individuelle.
        Valeur par défaut selon statistique MNTP.
        The default is 1.6.


    Returns
    -------
    Charge de trafic journalier moyen entrent dans la zone depuis l'extérieure de la zone d'exclusion.
    Nombre de personnes (pas le nombe de véhicule, hypothèse voiture = 1.6 de taux de remplissage).
    
        trafic='all' => int
            charge TIM + charge TP
            
        trafic='separate' => list(int,int,int)
        [charge TIM , charge TP , charge TIM + charge TP]  

    """
    
    # Arguments fonction
    
    if (trafic != 'all') and (trafic != 'separate'):
        raise ValueError("Argument 'trafic' pas valable !")
    
    else:
        
        # Dépendences
    
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import Point
        import psycopg2
        from sqlalchemy import create_engine
        from itertools import product
        
        import warnings
        warnings.filterwarnings("ignore")
        
        from gripit.DataBase import conStrPostgreSQL
        
        small = ri # rayon en km
        big = re # rayon en km
    
        pt = Point(coordinates)
        # pt = Point(2537878, 1152013) # Exemple gare Lausanne    
    
        dict_connection = conn
        
        taux = tim_taux # taux de remplissage, par défaut = 1.6
         
        # Fonctions internes    
        def getAttributs (sql, lst, dict_connection):
            conn = psycopg2.connect(
                host=dict_connection['hostname'],
                database=dict_connection['database'],
                user=dict_connection['username'],
                password=dict_connection['pwd'])
            
            attributs = pd.read_sql_query(SQLquery_flux, conn, params=[lst]) # Connexion à la base de données PostgreSQL via psycopg2
            conn.close() # Fermeture de la connexion à la base de données
            
            return attributs
    
        # Créations des zones
    
        small_pt = pt.buffer(small * 1000)
        big_pt = pt.buffer(big * 1000)
        
        
        data = {'id':['small', 'big'], 'r_km':[small, big], 'geometry':[small_pt, big_pt]}
        pts = gpd.GeoDataFrame(data, crs='EPSG:2056'); del data; del small_pt; del big_pt; del pt
    
        # Importation des zones MNTP depuis DB
    
        SQLquery_sjoin = '''
        SELECT id, geom FROM "MNTP"."zones"
        '''
        
        engine = create_engine(conStrPostgreSQL(dict_connection))
        zones = gpd.read_postgis(SQLquery_sjoin, engine).to_crs('EPSG:2056')
        engine.dispose()
        
        # Traitement data
        
        id_small = gpd.overlay(zones, pts[pts.id == 'small'], how='intersection')
        id_big = gpd.overlay(zones, pts[pts.id == 'big'], how='intersection')
        
        mask_inner = zones[zones.id.isin(id_small.id_1.tolist())]
        mask_outer = zones[-zones.id.isin(id_big.id_1.tolist())]
        
        lst = list(product(mask_inner.id.tolist(), mask_outer.id.tolist())) # Liste de tuple
        
        del mask_outer; del mask_inner; del zones; del id_small; del id_big
    
        # Importation des attributs de charge MNTP depuis DB       
            # Selon argument trafic
            
        SQLquery_flux = '''
        SELECT von, bis, value_tim, value_tp FROM "MNTP"."fulxCH"
        WHERE (von , bis) IN (SELECT * FROM unnest(%s) AS t(von integer, bis integer))
        '''
        
        attributs = getAttributs(SQLquery_flux, lst, dict_connection).fillna(0)
        
        lst_attribut = [round(attributs.value_tim.sum()), round(attributs.value_tp.sum())]
        lst_attribut[0] = round(lst_attribut[0] * taux)
        lst_attribut.append(lst_attribut[0] + lst_attribut[1])
    
        if trafic == 'all':            
            return lst_attribut[2]       
        
        elif trafic == 'separate':            
            return lst_attribut
        


def chargeTJM_betweenTwoPoint (origin: tuple, destination: tuple, r_Origin: int, r_Destination: int, conn: dict, trafic:str='all', tim_taux:float=1.6):    
    """
    Calcule la charge de trafic TJM entrent dans une zone circulaire de rayon défini, en nombre de voyageurs,
        depuis une autre zone circulaire d'origine. Exemple: agglomération Lausanne vers agglomération Genève.
    TIM => Taux journalier moyen

    Parameters
    ----------
    origin : tuple(int,int)
        Coordonnées géographique du centre de la zone d'origine.
        Coordonnées - EPSG:2056 (CH1903+ / LV95)
        Format (x,y) ou (lng,lat)
        
    destination : tuple(int,int)
        Coordonnées géographique du centre de la zone de destination.
        Coordonnées - EPSG:2056 (CH1903+ / LV95)
        Format (x,y) ou (lng,lat)
        
    r_Origin : int
        [km] Rayon de la zone circulaire d'origine.
        
    r_Destination : int
        [km] Rayon de la zone circulaire de destination.
        
    conn : dict
        Dictionaire de connection à la base de données,
        Schéma 'MNTP' dans le code source pour les valeurs de flux.
        Table 'fulxCH' dans le code source.
        
    trafic : str, optional
        trafic='all'
            Calcule la somme du trafic, total voiture et transport public.
        trafic='separate'
            Calcule le trafic total, voiture et transport public.
        The default is 'all'.

    tim_taux : float, optional
        Modifie le taux de remplissage des transport en voiture individuelle.
        Valeur par défaut selon statistique MNTP.
        The default is 1.6.


    Returns
    -------
    Charge de trafic journalier moyen entrent dans la zone depuis la zone d'origine.
    Nombre de personnes (pas le nombe de véhicule, hypothèse voiture = 1.6 de taux de remplissage).
    
        trafic='all' => int
            charge TIM + charge TP
            
        trafic='separate' => list(int,int,int)
        [charge TIM , charge TP , charge TIM + charge TP]  

    """
    
    # Arguments fonction
    
    if (trafic != 'all') and (trafic != 'separate'):
        raise ValueError("Argument 'trafic' pas valable !")
    
    else:
        
        # Dépendences
    
        import pandas as pd
        import geopandas as gpd
        from shapely.geometry import Point
        import psycopg2
        from sqlalchemy import create_engine
        from itertools import product
        
        import warnings
        warnings.filterwarnings("ignore")
        
        from gripit.DataBase import conStrPostgreSQL
        
        
        rO = r_Origin # rayon en km
        rD = r_Destination # rayon en km
    
        pt_O = Point(origin)
        pt_D = Point(destination)
        
        # pt = Point(2537878, 1152013) # Exemple gare Lausanne    
    
        dict_connection = conn
        
        taux = tim_taux # taux de remplissage, par défaut = 1.6
         
        # Fonctions internes
        
        def getAttributs (sql, lst, dict_connection):
            conn = psycopg2.connect(
                host=dict_connection['hostname'],
                database=dict_connection['database'],
                user=dict_connection['username'],
                password=dict_connection['pwd'])
            
            attributs = pd.read_sql_query(SQLquery_flux, conn, params=[lst]) # Connexion à la base de données PostgreSQL via psycopg2
            conn.close() # Fermeture de la connexion à la base de données
            
            return attributs
    
        # Créations des zones
    
        o_pt = pt_O.buffer(rO * 1000)
        d_pt = pt_D.buffer(rD * 1000)
        
        
        data = {'id':['O', 'D'], 'r_km':[rO, rD], 'geometry':[o_pt, d_pt]}
        pts = gpd.GeoDataFrame(data, crs='EPSG:2056'); del data; del o_pt; del d_pt; del pt_O; del pt_D
    
        # Importation des zones MNTP depuis DB
    
        SQLquery_sjoin = '''
        SELECT id, geom FROM "MNTP"."zones"
        '''
        
        engine = create_engine(conStrPostgreSQL(dict_connection))
        zones = gpd.read_postgis(SQLquery_sjoin, engine).to_crs('EPSG:2056')
        engine.dispose()
        
        # Traitement data
        
        data = gpd.overlay(zones, pts, how='intersection')
        
        id_o = data.id_1[data.id_2 == 'O'].unique().tolist()
        id_d = data.id_1[data.id_2 == 'D'].unique().tolist()
        
        lst = list(product(id_o, id_d)) # Liste de tuple
        
        del zones; del id_o; del id_d; del data
    
        # Importation des attributs de charge MNTP depuis DB       
            # Selon argument trafic
            
        SQLquery_flux = '''
        SELECT von, bis, value_tim, value_tp FROM "MNTP"."fulxCH"
        WHERE (von , bis) IN (SELECT * FROM unnest(%s) AS t(von integer, bis integer))
        '''
        
        attributs = getAttributs(SQLquery_flux, lst, dict_connection).fillna(0)
        
        lst_attribut = [round(attributs.value_tim.sum()), round(attributs.value_tp.sum())]
        lst_attribut[0] = round(lst_attribut[0] * taux)
        lst_attribut.append(lst_attribut[0] + lst_attribut[1])
    
        if trafic == 'all':            
            return lst_attribut[2]       
        
        elif trafic == 'separate':            
            return lst_attribut

