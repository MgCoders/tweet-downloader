#!/bin/python
# -*- coding: utf-8 -*-
from libs import TweetRepository,utils
from pymongo import MongoClient
from twython import Twython,TwythonError
import time
import sys
import datetime
import os
import Queue
import threading
import traceback
import pprint
script = os.path.basename(__file__)
#-----------------------------------------------------------------------
# Unqueue work
#-----------------------------------------------------------------------
def doTheHardWork(data,queue,minion):
    try:
        print minion,"Busco",data["id_str"],data["reason"]
        tweet = twitter.show_status(id=data["id_str"], tweet_mode = 'extended', include_entities = True)
        if tweet:
            tweetRepository.addTweets([tweet],data["reason"])
            ids_coll.delete_many({"id_str":data["id_str"]})
    except Exception, e:
    	traceback.print_exc(file=sys.stdout)
        print minion,"Error",data["id_str"],e
        if isinstance(e,TwythonError):
            if e.error_code == 404:
               ids_coll.delete_many({"id_str":data["id_str"]})
            if e.error_code == 429:
                if delay > 100:
                    delay = 10
                time.sleep(delay)
    #Minion is free again!
    queue.put(minion)
#-----------------------------------------------------------------------
# Database connection
#----------------------------------------------------------------------- 
credentials = {}
filename = os.path.join('/opt/libs', 'key_mongo.py')
exec(open(filename).read(), credentials)
tweetRepository = TweetRepository.TweetRepository(credentials["dbcoll"],credentials["dbdb"],credentials["mongostring"])
otherClient = MongoClient(credentials["mongostring"])
krypton_db = otherClient[credentials["dbdb"]]
ids_coll = krypton_db['tweets_ids_in_reply_to_status']
#-----------------------------------------------------------------------
# load our API credentials 
#-----------------------------------------------------------------------
config = {}
filename = os.path.join('/opt/libs', 'key_twitter_2.py')
exec(open(filename).read(), config)
#-----------------------------------------------------------------------
# create twitter API object
#-----------------------------------------------------------------------
twitter = Twython(config["consumer_key"], config["consumer_secret"],config["access_key"], config["access_secret"])
#-----------------------------------------------------------------------
# Params and Vars
#-----------------------------------------------------------------------
queue = Queue.Queue()
delay = 60
#-----------------------------------------------------------------------
# Infinite loop
#-----------------------------------------------------------------------
#Put minions to work
for minion in ["Minion-1","Minion-2"]:
    queue.put(minion)
#Forever
while True:
    minion = queue.get() #this will block until some minion is free
    print minion,'goes to work!'
    data = ids_coll.find_one({"consumed":False})
    if data:
        ids_coll.update_one({'_id': data['_id']}, {'$set': {'consumed': True}})
        thread = threading.Thread(target=doTheHardWork, args = (data,queue,minion))
        thread.daemon = True
        thread.start()
    else:
        queue.put(minion)
        print minion,'goes to sleep!'
        time.sleep(delay)
