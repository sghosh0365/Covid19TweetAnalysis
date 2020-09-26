import datetime
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy
import json
import pandas as pd
import csv
import re
from textblob import TextBlob
import string
import preprocessor as p
import os
import time
import zipfile
from TwitterLoadMongoDB import pushDatatoMongo
import TwitterCredentials
from TwitterGeolocator import determine_loc, remove_unicode, determine_lat_long


auth = OAuthHandler(TwitterCredentials.CONSUMER_KEY, TwitterCredentials.CONSUMER_SECRET)
auth.set_access_token(TwitterCredentials.ACCESS_TOKEN, TwitterCredentials.ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)


def scraptweets(search_words, date_since, numTweets, numRuns):
    tweet_json_export = []
    # Define a for-loop to generate tweets at regular intervals
    # We cannot make large API call in one go. Hence, let's try T times

    # Define a pandas dataframe to store the date:
    program_start = time.time()
    for i in range(0, numRuns):
        # We will time how long it takes to scrape tweets for each run:
        start_run = time.time()

        # Collect tweets using the Cursor object
        # .Cursor() returns an object that you can iterate or loop over to access the data collected.
        # Each item in the iterator has various attributes that you can access to get information about each tweet
        tweets = tweepy.Cursor(api.search, q=search_words, filter="retweets", lang="en", since=date_since, \
                               result_type='popular', tweet_mode='extended').items(numTweets)  
        # Store these tweets into a python list
        tweet_list = [tweet for tweet in tweets]
        noTweets = 0
        for tweet_status in tweet_list:
            tweet = json.dumps(tweet_status._json)
            tweet = json.loads(tweet)
            if ((tweet['coordinates'] or tweet['user']['location'] or tweet['place']) and (
                    not tweet['retweeted'] and 'RT @' not in tweet['full_text'])):  # o
                geo_loc_coord = []
                geo_loc_pl = []
                geo_loc_coord = []
                geo_loc_user = None
                tweet_url = None
                country_coord, state_coord, city_coord, zip_coord = None, None, None, None
                country_pl, state_pl, city_pl, zip_pl = None, None, None, None
                country_user, state_user, city_user, zip_user = None, None, None, None
                country, state, city, postcode = None, None, None, None
                latitude, longitude = None, None
                user_name = remove_unicode(tweet['user']['name']).replace("\n", "").replace("\r", "")
                user_screen_name = remove_unicode(tweet['user']['screen_name']).replace("\n", "").replace("\r", "")
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
                    tweet_text = tweet['full_text']
                    tweet_text = remove_unicode(tweet_text).replace("\n", "").replace("\r", "")
                    user_name = remove_unicode(tweet['user']['name']).replace("\n", "").replace("\r", "")
                    user_screen_name = remove_unicode(tweet['user']['screen_name']).replace("\n", "").replace("\r", "")
                    created_at = tweet['created_at']
                    created_at = datetime.datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').strftime(
                        '%Y-%m-%d %H:%M:%S')
                    if latitude is None or longitude is None:
                        place = city + ' ' + state + ' ' + country
                        place = place.strip()
                        latitude, longitude = determine_lat_long(place)
                        tweet_url = 'https://twitter.com/'+user_screen_name +'/status/'+tweet['id_str']
                    doc = {
                        "Type": "TopTweet",
                        "Tweet_ID": tweet['id_str'],
                        "Text": tweet_text,
                        "Created_At": created_at,
                        "Lang": tweet['lang'],
                        "User": {
                            "Name": user_name,
                            "Screen_Name": user_screen_name,
                            "Verified": tweet['user']['verified'],
                            "Profile_Image_url_https": tweet['user']['profile_image_url_https'],
                            "Retweet_Count": tweet['retweet_count'],
                            "Favorite_Count": tweet['favorite_count'],
                            "Tweet_url": tweet_url
                        },
                        "Geo_Location": {
                            "Country": country,
                            "State": state,
                            "City": city,
                            "Postcode": postcode,
                            "Tweet_Latitude": latitude,
                            "Tweet_Longitude": longitude
                        }
                    }
                    tweet_json_export.append(doc)
            noTweets += 1

        # Run ended:
        end_run = time.time()
        duration_run = round((end_run - start_run) / 60, 2)

        print('no. of tweets scraped for run {} is {}'.format(i + 1, noTweets))
        print('time take for {} run to complete is {} mins'.format(i + 1, duration_run))

    # Once all runs have completed, save them to a single csv file:
    # Obtain timestamp in a readable format
    to_json_timestamp = datetime.datetime.today().strftime('%Y-%m-%d')
    # Define working path and filename
    path = "C:\\Users\\TSG\\PycharmProjects\\TwitterCovid-19\\DataSets\\" # Change to your working directory
    filename_raw = path + 'Covid-19TopTweets.json'
    with open(filename_raw, 'w') as f:
        f.write(json.dumps(tweet_json_export, indent=4))
        filename = path + 'Covid-19TopTweets_' + to_json_timestamp + '.json'
        zipfilename = (filename.split('\\')[-1]).split('.')[0] + '.zip'
        archive_dir = "C:\\Users\\TSG\\PycharmProjects\\TwitterCovid-19\\DataSets\\archive" # Change to your working directory
        zipflnmdir = archive_dir + '\\' + zipfilename
        if os.path.exists(zipflnmdir):
            os.remove(zipflnmdir)
        zipfile.ZipFile(zipflnmdir, mode='w').write(filename_raw)
        f.close()
        collection = "TopTweetsCovid19"
        pushDatatoMongo(filename_raw, collection, 1, date_since)
    program_end = time.time()
    print('Scraping has completed!')
    print('Total time taken to scrap is {} minutes.'.format(round(program_end - program_start) / 60, 2))


# Initialise these variables:
now = datetime.datetime.now()
date_since = now - datetime.timedelta(days=1)
date_since = date_since.strftime("%Y-%m-%d")
search_words = "corona OR covid OR COVD19 OR COVID19 OR CoronavirusPandemic OR COVID-19 OR 2019nCoV OR CoronaOutbreak OR coronavirus OR WuhanVirus OR covid19 OR coronaviruspandemic OR covid-19 OR 2019ncov OR coronaoutbreak OR wuhanvirus OR pandemic OR quarantine OR ppe OR n95 OR sarscov2 OR nCov OR ncov2019"
numTweets = 2500
numRuns = 1
# Call the function scraptweets
scraptweets(search_words, date_since, numTweets, numRuns)
