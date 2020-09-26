# Covid19TweetAnalysis
A Python project to capture and analyze tweets on covid-19 in real time
Dear Programmer,

Good day to you!

I am Snehasis Ghosh, a Data Engineer by profession. I would like to share with you a recent project I did on covid-19 tweets data analysis. 

Introduction
============
This project captures the covid-19 tweets in real time across the globe. The extracted data is used to derive following analysis -

-> The density of tweets based on countries

-> The sentiment of the tweets

-> The actual covid-19 numbers

-> The top 10 trending tweets on covid-19

One of the interesting insights derived is that, there are more tweets from people living in the highly affected areas. Also, they have a much more negative sentiment in their tweets

The tweets are captured using Tweepy (An easy-to-use Python library for accessing the Twitter API www.tweepy.org) and the covid-19 numbers (deaths, recovered, infected etc.) were capture using the covid19api (https://api.covid19api.com/).
All the data are captured in real time using Python 3. The data is stored in MongoDB (in batches every 15 minutes) and analysis is published on Tableau dashboard. You can find the snapshots of the dashboards in the TableauDashboardSnaps branch for your reference

Technology Stack
================
Python3

MongoDB

Tableau

Pre-requisites
==============
Twitter Developer account (https://developer.twitter.com/en/apply-for-access)

Python 3

MongoDB or any other No SQL Database (optional - you can choose to have the data in json files)

Tableau or any other BI tool (optional - if you do not want to visualize the data)

Code Walkthrough
================
The main challenge was to extract and transform the data in real time. For this Python 3 has been used. I have uploaded all the Python files for you to try it, customize it and hopefully improve/add functionalities. Below are the scripts and their brief functionalities -

-> TwitterDataStream.py: This is the main script and it captures the tweets in real time

-> TwitterCredentials.py: This contains your twitter developer credentials

-> TweetSentimentAnalysis.py: This script determines the sentiment of the tweets

-> TwitterGeolocator.py: The Twitter user's geo location is not sometimes accurately captured in the Tweepy response json. This script calls the google map api to cleanse and accurately determine the geo location of the user

-> TwitterTopTweets.py: This script determines the top trending tweets on covid-19 from across the globe

-> TwitterLoadMongoDB.py: This loads the data into MongoDB

-> TwitterGeolocator.py: This collects the covid-19 numbers using the covid19api

Please reach out to me in case you need help or you have suggestions. I am open to feedback.

Regards,

Snehasis Ghosh

LinkedIn: https://linkedin.com/in/snehasis-ghosh-b17b30a3
