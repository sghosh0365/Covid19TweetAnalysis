from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import TwitterCredentials
import pandas as pd
import numpy as np
import json
import os
import datetime
import time
from shutil import copyfile, make_archive
import zipfile
import pipesubprocess as pipesub
import subprocess
from TwitterGeolocator import determine_loc, remove_unicode, determine_lat_long
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError
from TwitterLoadMongoDB import pushDatatoMongo
from TweetSentimentAnalysis import TweetSentiment


def stream_tweets(stream_tweets_filename, search_words):
    # This handles Twitter authentication and the connection to the Twitter streaming API
    listener = TwitterListener(stream_tweets_filename)
    auth = OAuthHandler(TwitterCredentials.CONSUMER_KEY, TwitterCredentials.CONSUMER_SECRET)
    auth.set_access_token(TwitterCredentials.ACCESS_TOKEN, TwitterCredentials.ACCESS_TOKEN_SECRET)
    stream = Stream(auth, listener)
    while True:
        try:
            stream.filter(track=search_words, stall_warnings=True, languages=['en']) 
        except (ProtocolError, AttributeError, IncompleteRead):
            continue


class TwitterListener(StreamListener):
    """
    This is a basic listener class that just prints received tweets to stdout
    """

    def __init__(self, fetched_tweets_file):
        self.fetched_tweets_file = fetched_tweets_file

    def on_data(self, data):
        try:
            tweet_document = {}
            tweet = json.loads(data)
            if ((tweet['coordinates'] or tweet['user']['location'] or tweet['place']) and (
                    not tweet['retweeted'] and 'RT @' not in tweet['text'])):  # Not considering retweets
                geo_loc_coord = []
                geo_loc_pl = []
                geo_loc_coord = []
                geo_loc_user = None
                longitude = None
                latitude = None
                country_coord, state_coord, city_coord, zip_coord = None, None, None, None
                country_pl, state_pl, city_pl, zip_pl = None, None, None, None
                country_user, state_user, city_user, zip_user = None, None, None, None
                country, state, city, postcode = None, None, None, None
                if tweet['truncated']:
                    tweet_text = tweet['extended_tweet']['full_text']
                else:
                    tweet_text = tweet['text']
                user_name = remove_unicode(tweet['user']['name']).replace("\n", "").replace("\r", "")
                user_screen_name = remove_unicode(tweet['user']['screen_name']).replace("\n", "").replace("\r", "")
                tweet_text = remove_unicode(tweet_text).replace("\n", "").replace("\r", "")
                if tweet['coordinates']:
                    longitude = tweet['coordinates'][0]
                    latitude = tweet['coordinates'][1]
                    geo_loc_coord.append(latitude)
                    geo_loc_coord.append(longitude)
                    country_coord, state_coord, city_coord, zip_coord = determine_loc(geo_loc_coord)
                if tweet['place']:
                    bbox = tweet['place']['bounding_box']['coordinates']
                    longitude = bbox[0][0][0]
                    latitude = bbox[0][0][1]
                    geo_loc_pl.append(latitude)
                    geo_loc_pl.append(longitude)
                    country_pl, state_pl, city_pl, zip_pl = determine_loc(geo_loc_pl)
                if tweet['user']['location']:
                    geo_loc_user = tweet['user']['location']
                    country_user, state_user, city_user, zip_user = determine_loc(geo_loc_user)
                if not (country_coord is None and state_coord is None and city_coord is None and zip_coord is None):
                    country, state, city, postcode = country_coord, state_coord, city_coord, zip_coord
                elif not (country_pl is None and state_pl is None and city_pl is None and zip_pl is None):
                    country, state, city, postcode = country_pl, state_pl, city_pl, zip_pl
                else:
                    country, state, city, postcode = country_user, state_user, city_user, zip_user
                if not (country is None and state is None and city is None and postcode is None):
                    polarity, subjectivity, score = TweetSentiment(tweet_text)
                    created_at = tweet['created_at']
                    created_at = datetime.datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').strftime(
                        '%Y-%m-%d %H:%M:%S')
                    if latitude is None or longitude is None:
                        place = city + ' ' + state + ' ' + country
                        place = place.strip()
                        latitude, longitude = determine_lat_long(place)
                    doc = {
                        "Type": "TweetStream",
                        "Tweet_ID": tweet['id_str'],
                        "Text": tweet_text,
                        "Created_At": created_at,
                        "Lang": tweet['lang'],
                        "Filter_Level": tweet['filter_level'],
                        "User": {
                            "Name": user_name,
                            "Screen_Name": user_screen_name,
                            "Verified": tweet['user']['verified']
                        },
                        "Geo_Location": {
                            "Country": country,
                            "State": state,
                            "City": city,
                            "Postcode": postcode,
                            "Tweet_Latitude": latitude,
                            "Tweet_Longitude": longitude
                        },
                        "Sentiment_Analysis": {
                            "Polarity": polarity,
                            "Subjectivity": subjectivity,
                            "Score": score
                        }
                    }
                    tweet_document.update(doc)
                    global tweet_json_dump
                    global start_time
                    tweet_json_dump.append(tweet_document)
                    curr_time = time.time()
                    global last_run_time
                    if round((curr_time - last_run_time) / 60, 2) > 120 or last_run_time == 0:
                        p = subprocess.Popen(["python.exe", 'TwitterTopTweets.py'])
                        q = subprocess.Popen(["python.exe", 'CoronaVirusFigures.py'])
                        last_run_time = curr_time

                    if (os.path.exists(self.fetched_tweets_file) and os.stat(self.fetched_tweets_file).st_size) / pow(
                            1024, 2) > 10 or round((curr_time - start_time) / 60, 2) > 240:  # (10MB or every 15 minutes)
                        # call mongodb import
                        filename = self.fetched_tweets_file
                        collection = "TwitterStreamCovid19"
                        pushDatatoMongo(filename, collection)
                        start_time = time.time()
                        tweet_json_dump = []  # Emptying the list
                        now = datetime.datetime.now()
                        filenm = self.fetched_tweets_file.split('.')[0]
                        extn = self.fetched_tweets_file.split('.')[1]
                        curr_dt = now.strftime("%Y-%m-%d_%H_%M")
                        filename_temp = filenm + '_' + curr_dt + '.' + extn
                        archive_filename = filenm.split(filenm.split('\\')[-1])[0] + 'archive\\' + \
                                           filename_temp.split('\\')[-1]
                        zipfilename = archive_filename.split('\\')[-1].split('.')[0] + '.zip'
                        archive_dir = archive_filename.split(archive_filename.split('\\')[-1])[0]
                        archive_dt = now.strftime("%Y-%m-%d")
                        archive_dir_dt = archive_dir + '\\' + archive_dt
                        chk_archive_dir_exists = os.path.isdir(archive_dir_dt)
                        if not chk_archive_dir_exists:
                            os.makedirs(archive_dir_dt)
                        zipfile.ZipFile(archive_dir_dt + '\\' + zipfilename, mode='w').write(self.fetched_tweets_file)

                    with open(self.fetched_tweets_file, 'w') as f:
                        f.write(json.dumps(tweet_json_dump, indent=4))
            return True
        except BaseException as e:
            print('Error on data: %s' % str(e))
        return True

    def on_error(self, status_code):
        print(status_code)


if __name__ == '__main__':
    hash_tag_list = ["corona", "covid", "COVD19", "COVID19", "CoronavirusPandemic", "COVID-19", "2019nCoV",
                     "CoronaOutbreak", "coronavirus", "WuhanVirus", "covid19", "coronaviruspandemic", "covid-19",
                     "2019ncov",
                     "coronaoutbreak", "wuhanvirus", "pandemic", "quarantine", "ppe", "n95", "sarscov2", "nCov",
                     "ncov2019"]																					 # List of hash tags
    tweet_json_dump = []
    last_run_time = 0
    start_time = time.time()
    fetched_tweets_filename = "C:\\Users\\TSG\\PycharmProjects\\TwitterCovid-19\\DataSets\\Covid-19TweetsStream.json" # Change to your working directory
    stream_tweets(fetched_tweets_filename, hash_tag_list)
