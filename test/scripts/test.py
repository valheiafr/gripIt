# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 17:00:51 2023

@author: valentin.pasche1
"""


## Avant chaque test, ouvrir une nouvelle consolle



""" Dictionnaire de connexion à la DB PostgreSQL - Spécifique à chaque utilisateur """
connexion = {
    "hostname" : 'pgsql01.gm.heia-fr.ch', # Adresse du serveur PostgreSQL
    "database" : 'gripit_DB', # Nom de la base de données (une DB est à l'intérieur d'un serveur)
    "username" : 'my_username', # Nom utilisateur
    "pwd" : 'my_mdp', # Mot de passe utilisateur
    "port_id" : 5432, # Port de communiquation avec le serveur (généralement 5432 avec PostgreSQL)
    "conn" : None, # Utile dans certains cas
    "schema" :'reseaux_GRIPIT'} # Dictionnaire de connexion à la DB PostgreSQL


""" Variables de test et chemins des fichiers de test """
network_numer = 4 # Numéro de l'implémentation, int ou float
file_path_s = r"C:\Users\valentin.pasche1\Desktop\test_pkg_gripit\test\datasets\stations_4.csv"
file_path_l = r"C:\Users\valentin.pasche1\Desktop\test_pkg_gripit\test\datasets\liaisons_4.csv"


## Test 1 - OK

import gripit as gp
gp.__version__
conn = gp.DataBase.conStrPostgreSQL(connexion)

## Test 2 - OK
from gripit import DataBase as db
conn = db.conStrPostgreSQL(connexion)

## Test 3 - OK
import gripit
conn = gripit.DataBase.conStrPostgreSQL(connexion)

## Test 4 - OK
from gripit.DataBase import newNetwork
stationsTable = newNetwork.creatStationsTable(file_path_s)
connectionsTable = newNetwork.creatLinksTable(file_path_l)

## Test 5 - OK
import gripit as gp
stationsTable = gp.DataBase.creatStationsTable(file_path_s)

## Test 6 - OK
import gripit as gp
v = gp.Physics.tGripit(100, 10)

## Test 7 - OK
import gripit as gp
v = gp.Physics.timePath.tGripit(100, 10)

## Test 8 - OK
from gripit.Physics import tGripit
v = tGripit(100, 10)

## Test 9 - OK
from gripit.DataBase import fluxCH as fx

gare_o = (2537878, 1152013) # Gare Lausanne
gare_d = (2499968, 1118531) # Gare Genève

x = fx.chargeTJM_onePoint(gare_o,20,20,dict_connection, trafic='separate')
y = fx.chargeTJM_betweenTwoPoint(gare_d,gare_o,10,10,dict_connection, trafic='separate')
z = fx.chargeTJM_betweenTwoPoint(gare_d,gare_o,10,10,dict_connection, trafic='separate', tim_taux=1)




