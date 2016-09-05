import json
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from unidecode import unidecode
from pymongo import MongoClient
import mysql.connector
from mysql.connector import errorcode
import folium

#Twitter Credentials
ckey = "your ck here"
csecret = "your csecret here"
atoken = "your atoken here"
asecret = "your atoken secret here"


OutMongo=False
OutMySQL=False
OutFile=False
OutConsole=True
OutMap=True
FilteredByLocation=True


# córdoba city rectangle coordinates. See http://boundingbox.klokantech.com/ (TSV format)
cordoba = [-4.8422241211, 37.8537121226, -4.7254943848, 37.9219928312]

#txt file 
if OutFile:
    file = open('tweets.txt', 'a')

if OutMongo:
    #local  mongodb server
    client = MongoClient('localhost', 27017)
    #dbname 
    db = client['twitter_db']
    #colección twitter_collection
    collection = db['twitter_collection']

if OutMap:
    map_1 = folium.Map (location=[40.4315829, -3.81804859], zoom_start=12, tiles='Stamen Terrain')

#MYSQL
#A 'twitter' db is supposed to be already created in MySQL with the following table 
    #SQL = (
    #    "CREATE TABLE `twitter` ("
    #    "  `user` varchar(100) NOT NULL,"
    #    "  `location` varchar(100) NOT NULL,"
    #    "  `tweet` varchar(150) NOT NULL,"
    #    "  `created` varchar(50) DEFAULT NULL"
    #    ") ENGINE=InnoDB" )
   

class listener(StreamListener):
    def on_data(self, data):
        # Twitter returns data in JSON format - we need to decode it first
        try:
            decoded = json.loads(data)
        except Exception as e:
            print (e)  # we don't want the listener to stop
            return True

        if decoded.get('geo') is not None:
            location = decoded.get('geo').get('coordinates')
            text=unidecode(decoded['text'])
            text=text.translate(str.maketrans({"'":None}))
            text=text.translate (str.maketrans ({'"': None}))
            user = '@' + decoded.get('user').get('screen_name')
            created = decoded.get('created_at')
            #old format style
            tweet = '%s|%s|%s|%s\n' % (user, location, text, created )
            if OutConsole:
                print(tweet)
            if OutMongo:
                # mongodb inserts
                collection.insert(decoded)
            if OutFile:
                #file appends
                file.write (tweet)
            #if OutConsole:
            #    print(text)
            if OutMySQL:
                # mysql
                cnx = mysql.connector.connect (user='root', host='localhost', database='twitter', use_pure=False)
                cursor = cnx.cursor ()
                data = (user, location, text, created)
                add_tweet_msqyl = 'INSERT INTO twitter (user, location, tweet, created) VALUES' + '(\'{s[0]}\',\'{s[1]}\',\'{s[2]}\',\'{s[3]}\')'.format (s=data)
                # print(add_tweet_msqyl)
                cursor.execute (add_tweet_msqyl)
                cnx.commit ()
                cnx.close ()
            if OutMap:
                location = decoded.get ('geo').get ('coordinates')
                text = unidecode (decoded['text'])
                folium.Marker(location, popup=text, icon=folium.Icon (icon='cloud')).add_to (map_1)
                map_1.save (outfile='map.html')
        else:
            location = '[,]'
            text = unidecode(decoded['text'])
            #if OutConsole:
            #    print('NO:     '+ text)
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    print('Starting')
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    #Filtered by track and location
    fTrack='Messi'
    if FilteredByLocation:
        twitterStream = Stream(auth, listener()).filter(track=fTrack,locations=cordoba)
    else:
        twitterStream = Stream (auth, listener ()).filter(track=fTrack)


