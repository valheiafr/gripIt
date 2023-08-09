from .edit import *
from .newNetwork import *
from .fluxCH import *


def conStrPostgreSQL (dict_conn):
    """
    Crée la clé d'accès à la base de données PostgreSQL.

    Parameters
    ----------
    dict_conn : dict
        Dictionnaire de connexion,
        avec mdp, adresse, etc. .

    Returns
    -------
    connection_string : str
        Clé d'accès sous forme de chaine de caractère.

    """
    # Adresse du serveur PostgreSQL [str] : Port d'entrée [int] (défaut 5432)
    host = f'''{dict_conn["hostname"]}:{dict_conn["port_id"]}'''
    # Nom de la base de données [str]
    database = dict_conn["database"]
    # Nom d'utilisateur PostgreSQL [str]
    user = dict_conn["username"]
    # Mot de passe de l'utilisateur PostgreSQL
    password = dict_conn["pwd"]
    # Chaine de connexion complète [str]
    return f"postgresql://{user}:{password}@{host}/{database}"
