#!/bin/python
# -*- coding: utf-8 -*-
from pymongo import MongoClient
import pymongo
from utils import simplifyText,getTokens
import json
import datetime
from random import randint
from bson.objectid import ObjectId


class Repository(object):
    """
    Clase para operaciones con colecciones de datas.
    """
    
    def __init__(self,dbcoll,dbdb,mongostring):
        self.client = MongoClient(mongostring)
        self.db_name = dbdb
        self.data_coll_name = dbcoll
        self.entity_coll_name = "entidades-candidatas"
        self.reply_to_coll_name = "tweets_ids_in_reply_to_status"
        self.discarded_coll_name = "krypton-discarded-data"
        self.client.server_info()
        
    def getDataCollection(self):
        return self.client[self.db_name][self.data_coll_name]
        
    def getDiscardedCollection(self):
        return self.client[self.db_name][self.discarded_coll_name]
        
    def getReplyToCollection(self):
        return self.client[self.db_name][self.reply_to_coll_name]
    
    def getEntityCollection(self):
        return self.client[self.db_name][self.entity_coll_name]
     
    def findData(self,criteria=""):
        """
        Devuelve todos como lista
        """
        return list(self.getDataCollection().find(criteria))
  
    def add(self, data):
        """
        Agrega uno si no existe
        """
        # try to find data
        try:
            origin = data['origin']
            origin_id = data["origin_id"]
            cursor = self.getDataCollection().find({ 'origin_id': origin_id, 'origin': origin}, limit=1)
            # do nothing if data exists already
            if cursor.count() > 0:
                cursor.close()
                return 0
            cursor.close()
        except Exception, e:
            print "Query failed finding data"
        # insert data
        try:
            self.getDataCollection().insert_one(data)
            return 1
        except Exception, e:
            print "Failed to insert data: ", e
            return 0
    
    
    def getRandomOne(self,field,value):
        """
        Dado un field y un value encuentra uno random que cumple.
        """
        unclassified = self.findData({field: value})
        if len(unclassified) > 0:
            return unclassified[randint(0,len(unclassified) - 1)]
        else:
            raise Exception('Failed, no data matching criteria')
    

    def updateOne(self,id,field,value):
        """
        Actualiza field con value para el que tiene _id = id.
        """
        return self.getDataCollection().update_one({'_id': ObjectId(id) }, {'$set': {field: value}})
        
    def saveEntities(self,entities):
        """
        Actualiza las entities candidatas.
        """
        for par in entities:            
            candidate = self.getEntityCollection().find_one({'entity': par['entity'].upper()})
            if candidate and par['class'] != 'Ninguna':
                self.getEntityCollection().update_one({'entity': par['entity'].upper() }, {'$set': {'class': par['class']}})
            elif candidate:
                self.getEntityCollection().delete_one({'entity': par['entity'].upper()})
            elif par['class'] != 'Ninguna':
                self.getEntityCollection().insert_one({'entity': par['entity'].upper(), 'class': par['class']})
            
    def getEntities(self,text):
        """
        Traigo entidades y clases candidatas de un diccionario en la base
        """
        result = []
        prev_pos = 0
        for token in getTokens(text):
            candidate = self.getEntityCollection().find_one({'entity': token.upper()})
            pos = text.find(token,prev_pos)
            prev_pos = pos
            length = len(token)
            if candidate and pos > -1 and length > 0:
                result.append({'entity':token, 'class':candidate['class'], 'pos':pos, 'len':length})
            elif pos > -1 and length > 0:
                result.append({'entity':token, 'class':'Ninguna', 'pos':pos, 'len':length})
        return result
    
    def getInfoNer(self):
        """
        Info de cantidades NER
        """
        info = {}
        info["total"] = self.getDataCollection().find().count()
        info["ner_classified"] = self.getDataCollection().find({"ner_classified":True}).count()
        return info
    
    def getInfoKrypton(self):
        """
        Info de cantidades Krypton
        """
        info = {}
        info["total"] = self.getDataCollection().find().count()
        info["krypton_classified"] = self.getDataCollection().find({"krypton_classified":True}).count()
        categorias = ['NO_CLASIFICADO', 'NO_UTIL', 'POCO_UTIL', 'UTIL', 'MUY_UTIL']
        info['krypton_categories'] = {}
        for cat in categorias:
            count_cat = self.getDataCollection().find({"krypton_classified":True,"krypton_category":cat}).count()
            info['krypton_categories'][cat] = count_cat
        return info
        
    def updateWithEntities(self,info_words):
        """
        Actualiza la lista de pares palabra/clase
        """
        dicc_tweets = {}
        for (word,label,id_tweet) in info_words:
            if label != 0 and label != '0':
                dicc_entities = dicc_tweets.get(id_tweet,{})
                list_entities = dicc_entities.get(label,[])
                list_entities.append(word)
                dicc_entities[label] = list_entities
                dicc_tweets[id_tweet] = dicc_entities
            
        for id_tweet in dicc_tweets.keys():
            tweet = self.getTweetById(id_tweet)
            dicc_entities = dicc_tweets.get(id_tweet,{})
            tweet['entities'].update(dicc_entities)
            self.updateOne(id_tweet,'entities',tweet['entities'])
            
        return self.getAll()
        
        

class TweetRepository(Repository):
    """
    Clase para operaciones con colecciones de tweets
    """
    
    def addTweetInReplyToId(self,data):
        """
        Si tiene referencia a Tweet original lo guardo en una lista.
        """
        if data.get(u'in_reply_to_status_id_str',False):
            cursor = self.getReplyToCollection().find({'id_str': data[u'in_reply_to_status_id_str']}, limit=1)
            # do nothing if data exists already
            if cursor.count() == 0:
                self.getReplyToCollection().insert_one({"id_str":data[u'in_reply_to_status_id_str'],"reason":"ReplyTo","consumed":False})
                
    def addRetweetId(self,data):
        """
        Si tiene referencia a Retweet.
        """
        if data.get(u'retweeted_status',False):
            cursor = self.getReplyToCollection().find({'id_str': data[u'retweeted_status'][u'id_str']}, limit=1)
            # do nothing if data exists already
            if cursor.count() == 0:
                self.getReplyToCollection().insert_one({"id_str":data[u'retweeted_status'][u'id_str'],"reason":"Retweet","consumed":False})
                
    def addQuotedId(self,data):
        """
        Si tiene referencia a Quoted.
        """
        if data.get(u'is_quote_status',False):
            cursor = self.getReplyToCollection().find({'id_str': data[u'quoted_status'][u'id_str']}, limit=1)
            # do nothing if data exists already
            if cursor.count() == 0:
                self.getReplyToCollection().insert_one({"id_str":data[u'quoted_status'][u'id_str'],"reason":"Quoted","consumed":False})
                    
    def addTweets(self, tweets, search_criteria=None):
        """
        Guardo versión simplificada del tweet
        """
        insertInfo = {}
        insertInfo['count'] = 0
        insertInfo['lastId'] = 0
        for tweet in tweets:
            data = {}
            #Global info
            data[u'origin']='TWEET';
            data[u'origin_id'] = tweet[u'id_str']
            data[u'search_criteria'] = search_criteria
            #Extended tweets
            if tweet.get(u'extended_tweet',False):
                tweet[u'text'] = tweet[u'extended_tweet']['full_text']
            if tweet.get(u'full_text',False):
                tweet[u'text'] = tweet['full_text']

            data[u'text'] = simplifyText(tweet[u'text'])
            data[u'created_at'] = tweet[u'created_at']
            data[u'imported_at'] = datetime.datetime.now()
            data[u'krypton_classified'] = False
            data[u'krypton_category'] = None
            data[u'krypton_stanfordtagged'] = False
            data[u'krypton_imagetagged'] = False
            data[u'krypton_geotagged'] = False
            data[u'krypton_unsupervised_classified'] = False 
            data[u'krypton_domain_reclamos_classified'] = False
            data[u'krypton_event_classified'] = False
            data[u'krypton_events_classified_rf'] = False
            #Specific Twitter info
            data[u'tw_user'] = {}
            data[u'tw_user'][u'id_str'] = tweet[u'user'][u'id_str']
            data[u'tw_user'][u'name'] = tweet[u'user'][u'name']
            data[u'tw_user'][u'screen_name'] = tweet[u'user'][u'screen_name']
            data[u'tw_user'][u'profile_image_url'] = tweet[u'user'][u'profile_image_url']
            data[u'tw_user'][u'profile_image_url']
            data[u'tw_favorite_count'] = tweet[u'favorite_count']
            data[u'tw_retweet_count'] = tweet[u'retweet_count']
            data[u'tw_in_reply_to_status_id_str'] = tweet[u'in_reply_to_status_id_str']
            data[u'tw_entities'] = tweet[u'entities']
            data[u'tw_location'] = tweet[u'user'][u'location']
            data[u'tw_coordinates'] = tweet[u'coordinates']
            #In reply to
            self.addTweetInReplyToId(tweet)
            #Quoted
            self.addQuotedId(tweet)
            #Retweeted
            self.addRetweetId(tweet)
            #Insert if not RT
            if not tweet.get(u'retweeted_status',False):
                insertInfo['count'] += self.add(data)
                insertInfo['lastId'] = data[u'origin_id']
        return insertInfo



class ClaimRepository(Repository):
    """
    Clase para operaciones con colecciones de reclamos de la IMM
    """
                
    def addClaims(self, claims):
        """
        Guardo versión simplificada del reclamo
        """
        insertInfo = {}
        insertInfo['count'] = 0
        insertInfo['lastId'] = None
        for claim in claims:
            data = {}
            #Global info
            data[u'origin']='RECLAMOS';
            data[u'origin_id'] = claim[u'id']
            data[u'text'] = simplifyText(claim[u'observaciones']) + ". " + claim[u'ubicacion']
            data[u'created_at'] = str(claim[u'dia_ingreso']) + '/' + str(claim[u'mes_ingreso']) + '/' + str(claim[u'anio_ingreso'])
            data[u'imported_at'] = datetime.datetime.now()
            data[u'krypton_classified'] = True
            if claim[u'area'] == 'Limpieza':
                data[u'krypton_category'] = 'MUY_UTIL'
            else:
                data[u'krypton_category'] = 'NO_UTIL'

            data[u'ner_classified'] = False
            data[u'ner_entities'] = []
            data[u'krypton_geotagged'] = False
            data[u'krypton_imagetagged'] = True
            #Specific Claim info
            data[u'claim_area'] = claim[u'area']
            data[u'claim_location'] = claim[u'ubicacion']
            data[u'claim_esp_location'] = claim[u'especif_ubicacion']
            data[u'claim_problem_type'] = claim[u'tipo_problema']
            #Insert info
            insertInfo['count'] += self.add(data)
        return insertInfo

    
   
