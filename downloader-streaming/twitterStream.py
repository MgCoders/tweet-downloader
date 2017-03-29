from twython import TwythonStreamer
from libs import TweetRepository,utils
import os
from pprint import pprint

class TweetStreamer(TwythonStreamer):
    def on_success(self, data):
        if 'text' in data:
            insertInfo = tweetRepository.addTweets([data],'Streaming api')
            if insertInfo['count'] > 0:
                info = {'id':data['id_str'],
                        'date':data['created_at'],
                        'text':data['text'],
                        'user':data[u'user'][u'name']}
                print pprint(info)

    def on_error(self, status_code, data):
        print status_code,data
        #self.disconnect()
 
#-----------------------------------------------------------------------
# Database connection
#-----------------------------------------------------------------------           
credentials = {}
filename = os.path.join('/opt/libs', 'key_mongo.py')
exec(open(filename).read(), credentials)
tweetRepository = TweetRepository.TweetRepository(credentials["dbcoll"],credentials["dbdb"],credentials["mongostring"])

#-----------------------------------------------------------------------
# load track words, space is and, comma is or 
# https://dev.twitter.com/streaming/overview/request-parameters#track
#-----------------------------------------------------------------------        
filename = os.path.join('/opt/libs', 'palabras_clave_dominio.txt')        
trackwords_domain = open(filename).read().splitlines()
filename = os.path.join('/opt/libs', 'palabras_clave_ciudad.txt')   
trackwords_city = open(filename).read().splitlines()     
or_track = []
for trackdomain in trackwords_domain:
    for trackcity in trackwords_city:
        strip_trackdomain = trackdomain.strip()
        strip_trackcity = trackcity.strip()
        if strip_trackdomain and strip_trackcity:
            domain_and_city = ' '.join([trackdomain,trackcity])
            or_track.append(domain_and_city)
tracklist = ','.join(or_track)
print 'Track:',tracklist
#-----------------------------------------------------------------------
# load city bounding box
# https://dev.twitter.com/streaming/overview/request-parameters#locations
#-----------------------------------------------------------------------        
filename = os.path.join('/opt/libs', 'city_bounding_box.txt')        
bounding_box = open(filename).read()
print 'Boundig Box',bounding_box
#-----------------------------------------------------------------------
# load our API credentials 
#-----------------------------------------------------------------------
config = {}
filename = os.path.join('/opt/libs', 'key_twitter_1.py')
exec(open(filename).read(), config)
#-----------------------------------------------------------------------
# create twitter API object
#-----------------------------------------------------------------------
streamer = TweetStreamer(config["consumer_key"], config["consumer_secret"],config["access_key"], config["access_secret"])    
streamer.statuses.filter(languages=["es"],tweet_mode = 'extended', track = tracklist)
#streamer.statuses.filter(languages=["es"],track = tracklist,locations = str(bounding_box))

