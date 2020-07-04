#!/usr/bin/env python

import os
import json

import boto3
import tweepy

import base64


consumer_key = os.getenv("consumer_key")
consumer_secret = os.getenv("consumer_secret")

access_token = os.getenv("access_token")
access_token_secret = os.getenv("access_token_secret")

access_key = os.getenv("access_key")
secret_key = os.getenv("secret_key")


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

kinesis_client = boto3.client('kinesis', region_name='us-east-1', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

class KinesisStreamProducer(tweepy.StreamListener):

        def __init__(self, kinesis_client):
                self.kinesis_client = kinesis_client

        def on_data(self, data):
                tweet = json.loads(data)
                tweets_data = json.dumps(tweet)
                self.kinesis_client.put_record(StreamName='KinesisDemo', Data=tweets_data, PartitionKey="key")
                print("Publishing record to the stream: ", tweet)
                return True

        def on_error(self, status):
                print("Error: " + str(status))

def main():
        mylistener = KinesisStreamProducer(kinesis_client)
        myStream = tweepy.Stream(auth = auth, listener = mylistener)
        myStream.filter(track=['#COVID19'])

if __name__ == "__main__":
        main()
