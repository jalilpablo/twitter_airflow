from turtle import Screen
import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    API_Key='...'
    API_Key_Secret='...'

    Access_Token='...'
    Access_Token_Secret='...'

    #Twitter Authentication
    auth = tweepy.OAuthHandler(API_Key,API_Key_Secret)
    auth.set_access_token(Access_Token,Access_Token_Secret)

    #API object
    api=tweepy.API(auth)

    #Select twitter user and other settings
    tweets = api.user_timeline(screen_name='@elonmusk',  #Tweets from Musk
                            count=200, #Maximum allowed count, how many tweets want to extract
                            include_rts = False, #only own tweets
                            tweet_mode='extended' #full text, not only first 140 char
                            )

    tweet_list=[] #empty list to be filled
    for tweet in tweets:
        text=tweet._json['full_text']
        refined_tweet = {   #dicc with relevant information
            'user': tweet.user.screen_name,
            'text': text,
            'fav_count': tweet.favorite_count,
            'rtw_count': tweet.retweet_count,
            'created_at': tweet.created_at
        }
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('s3://twitter-airflow/musk_twitter_data.csv')  #store it on s3 bucket

