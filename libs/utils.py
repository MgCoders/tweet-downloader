#!/usr/bin/python
# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
import json
import os
from twitter import *
import re
import unicodedata

def twitterAPI():
    #-----------------------------------------------------------------------
    # load our API credentials 
    #-----------------------------------------------------------------------
    config = {}
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'config.py')
    exec(open(filename).read(), config)

    #-----------------------------------------------------------------------
    # create twitter API object
    #-----------------------------------------------------------------------
    return Twitter(auth = OAuth(config["access_key"], config["access_secret"], config["consumer_key"], config["consumer_secret"]))



#Dado un conjunto de datos, luego de utilizar el tokenizador de nltk
#devuelve una lista de las n palabras mas frecuentes del conjunto ordenadas por
#frecuencia
def mostFrequentWords (n,data,searchList = None):
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'stopwords.txt')
    #Elimino de los datos las stopwords
    stopwords = open(filename).read().splitlines()
    data = filterWords(data,stopwords, True)
    
    if searchList != None:
        data = filterWords(data,searchList,True)
    
    words = {}
    for tweet in data:
        for word in tweet.split():
            word = simplifyText(word)
            word = re.sub('[\-\.\,\$\%\^\&\*\s\:]+', '', word)
            if word and word not in searchList:
                if word.lower() in words: 
                    words[word.lower()] = words[word.lower()] + 1
                else:
                    words[word.lower()] = 1

    mostFrequent = [w for w in sorted(words, key=words.get, reverse=True) if words[w] > 10]
    return mostFrequent[:n]


#Dado un conjunto de datos, una lista de palabras, y un booleano
#filtra el conjunto de los datos segun las palabras de la lista.
#Si not_in = False: Se queda con las palabras de los datos que pertenecen a la lista
#Si not_in = True: Se queda con las palabras de los datos que NO pertencen a la lista
def filterWords(data,filter,not_in):
    #print filter
    filtered_data = []
    for d in data:
        if(not_in):
            filt = [w.lower() for w in d.split() if not simplifyText(w.lower()) in filter]
        else:
            filt = [w.lower() for w in d.split() if simplifyText(w.lower()) in filter]
        
        if len(filt) != 0:
            filtered_data.append(" ".join(filt))
    
    return filtered_data


def simplifyText(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    text = re.sub(' +',' ',text)
    return unidecode(text)
    

def getTokens(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    import string
    #Saco urls, dejo espacio
    text = re.sub('http(.)*',' ',text)
    #Saco todo lo que no sea palabra, dejo espacio
    text = re.sub('[^#@a-zA-Z0-9_]',' ',text)
    #Saco espacios de mas
    text = re.sub(' +',' ',text)
    text = unidecode(text)
    return text.split(' ')

def getTokensNoUserNoHashtag(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    import string
    #Saco urls, dejo espacio
    #text = strip_accents(text)
    text = text.strip()
    text = re.sub('http(.)*',' ',text)
    #Saco todo lo que no sea palabra, dejo espacio
    text = re.sub('[^#a-zA-Z0-9_]',' ',text) 
    #Busco hashtag
    hashtags = re.findall(r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", text)
    new_words = []
    for hashtag in hashtags:
        hashtag = re.sub('#', '', hashtag)
        for word in camel_case_split(hashtag):
            new_words.append(word)
    text += ' '.join(new_words)
    text = re.sub(r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",' ',text) 
    #Saco espacios de mas
    text = re.sub(' +',' ',text)
    #text = unidecode(text)
    text = text.strip()
    return text.split(' ')

def eliminarHashtagsYUrl(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    import string
    #Saco urls, dejo espacio
    text = text.strip()
    text = re.sub('http(.)*',' ',text)
    #Saco todo lo que no sea palabra, dejo espacio
    text = re.sub('[^@a-zA-Z0-9_]',' ',text) 
    #Busco hashtag
    hashtags = re.findall(r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", text)
    for hashtag in hashtags:
        text = re.sub(hashtag, ' ', text)
    #Saco espacios de mas
    text = re.sub(' +',' ',text)
    text = text.strip()
    return text

def eliminarHashtagsYMentionsYUrl(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    import string
    #Saco urls, dejo espacio
    text = text.strip()
    text = re.sub('http(.)*',' ',text)
    #Busco mentions
    mentions = re.findall(r"(?:\@+[\w_]+[\w\'_\-]*[\w_]+)", text)
    for mention in mentions:
        text = re.sub(mention, ' ', text)
    #Saco todo lo que no sea palabra, dejo espacio
    text = re.sub('[^#a-zA-Z0-9_]',' ',text) 
    #Busco hashtag
    hashtags = re.findall(r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", text)
    for hashtag in hashtags:
        text = re.sub(hashtag, ' ', text)
    #Saco espacios de mas
    text = re.sub(' +',' ',text)
    text = text.strip()
    return text

def preprocesarTexto(text,raw=False,noHashTag=False,noMention=True):
    if raw:
        return text
    if noHashTag and noMention:
        return eliminarHashtagsYMentionsYUrl(text)
    if noHashTag:
        return eliminarHashtagsYUrl(text)
    if noMention:
        return eliminarMentionsYUrl(text)

def eliminarMentionsYUrl(text):
    """
    Saca tildes, Saca espacios extras
    """
    from unidecode import unidecode
    import re
    import string
    #Saco urls, dejo espacio
    text = text.strip()
    text = re.sub('http(.)*',' ',text)
    #Busco mentions
    mentions = re.findall(r"(?:\@+[\w_]+[\w\'_\-]*[\w_]+)", text)
    for mention in mentions:
        text = re.sub(mention, ' ', text)
    #Saco todo lo que no sea palabra, dejo espacio
    text = re.sub('[^#a-zA-Z0-9_]',' ',text) 
    #Saco espacios de mas
    text = re.sub(' +',' ',text)
    text = text.strip()
    return text
    
def camel_case_split(identifier):
    import re
    matches = re.finditer('.+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)', identifier)
    return [m.group(0) for m in matches]

def convertHashtag(token):
    if not token or len(token)==0:
        return ''
    res = []
    if token.startswith("#"):
        token = re.sub('#', '', token)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', token)
    parts = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower().split('_')
    for p in parts:
        res.append(convertHashtag(p))
    if len(res)==0:
        res.append(token)
    return res

def getTerminosComunesDominioDict():
    result = {}
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'palabras_comunes_dominio.txt')
    abvs = open(filename).read().splitlines()
    for abv in abvs:
        result[strip_accents(abv.lower())] = True
    return result

def getCuentasNoticiasList():
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'cuentas_fuentes_de_noticias.txt')
    abvs = open(filename).read().splitlines()
    return abvs

def getStopWordDict():
    result = {}
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'stopwords.txt')
    stopwords = open(filename).read().splitlines()
    for stopword in stopwords:
        result[strip_accents(stopword.lower())] = True
    return result

def getTerminosBusquedaDict():
    result = {}
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'palabras_clave_ciudad.txt')
    stopwords = open(filename).read().splitlines()
    filename = os.path.join(here, 'palabras_clave_dominio.txt')
    stopwords += open(filename).read().splitlines()
    texto_armado = ' '.join(stopwords)
    tokens = getTokensNoUserNoHashtag(texto_armado)
    for token in tokens:
        result[token.lower()] = True
    return result

def midpoint(x1, y1, x2, y2):
    #Input values as degrees
    import math
    #Convert to radians
    lat1 = math.radians(x1)
    lon1 = math.radians(x2)
    lat2 = math.radians(y1)
    lon2 = math.radians(y2)
    bx = math.cos(lat2) * math.cos(lon2 - lon1)
    by = math.cos(lat2) * math.sin(lon2 - lon1)
    lat3 = math.atan2(math.sin(lat1) + math.sin(lat2), \
           math.sqrt((math.cos(lat1) + bx) * (math.cos(lat1) \
           + bx) + by**2))
    lon3 = lon1 + math.atan2(by, math.cos(lat1) + bx)
    return [round(math.degrees(lat3), 2), round(math.degrees(lon3), 2)]

def geoDistance(coords1,coords2):
    from geopy.distance import vincenty
    (lon1,lat1) = coords1
    coords1 = (lat1,lon1)
    (lon2,lat2) = coords2
    coords2 = (lat2,lon2)
    return vincenty(coords1, coords2).m
    
def getCityBoundingBox():
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'city_bounding_box.txt')
    bounding_box = open(filename).read()
    result = []
    for coord in bounding_box.split(','):
        result.append(float(coord))
    return result

def agregarTokenProblematicoGeo(newline):
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'tokens_problematicos_geo.txt')
    hs = open(filename,"a")
    hs.write(newline)
    hs.close() 

def strip_accents(text):
    import re
    import unicodedata
    """
    Strip accents from input String.

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    try:
        text = unicode(text, 'utf-8')
    except NameError: # unicode is a default on python 3 
        pass
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    return str(text)
    
def getClasificationModel(filename):
    import cPickle
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, filename)
    return cPickle.load(open(filename, "rb"))
    
