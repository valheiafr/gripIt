# -*- coding: utf-8 -*-
"""
Created on Wed May 31 14:57:55 2023

@author: valentin.pasche1
"""

import numpy as np


def tGripit (distance, vMax, acceleration=1, deceleration=1) :
    """
    Temps de parcours selon MRUA
    Liaisons Gripit Lower/Main/Higher

    Parameters
    ----------
    distance : int/float
        Longeur du tronçon [m].
    vMax : int
        Vitesse maximum, vitesse contante [km/h].
    acceleration : int/float, optional
        Accélération constante [m/s2]. The default is 1.
    deceleration : int/float, optional
        Décélération constante [m/s2]. The default is 1.

    Returns
    -------
    float
        Temps de parcours [min].

    """
    
    x = distance # [m]
    v = vMax / 3.6 # [m/s]
    a = acceleration # [m/s2]
    d = deceleration # [m/s2]
    
    taMax = v/a # Temps d'accélération maximum [s]
    tdMax = v/d # Temps de décélération maximum [s]
    dSadMax = (taMax + tdMax)*(v/2) # Distance des rampes cumulées tdMax et taMax [m]
    
    if x > dSadMax : # Si phase à vitesse constante
        tVc = (x - dSadMax)/v # Temps à vitesse constante [s]
        t = tVc + taMax + tdMax # Temps total [s]
    else : # Si uniquement accélération et décélération
        td = np.sqrt(2*x/((d**2/a)+d)) # Temps de décélération [s]
        ta = td*(d/a) # Temps d'accélération [s]
        t = ta + td # Temps total [s]
        
    return round((t/60),1) # Temps total [min], arrondi à 6 [s]
