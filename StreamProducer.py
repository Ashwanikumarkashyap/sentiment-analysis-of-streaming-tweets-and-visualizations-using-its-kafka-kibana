from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

# TWITTER API CONFIGURATIONS
consumer_key = "Tba4dz6iAhJ8aIlruIVyOgv1H"
consumer_secret = "hrMWxGtZPKjDPhWz6iPVRIXnrJAFofRqW8cCh3lQG9TKXSNUEs"
access_token = "1250167730981998592-RVTbFdCcrbvxl4VWOPh1MMrur0Co0w"
access_secret = "n5K7kaJDMtMChQBqoMtPX957NJNdOyj1nY1O9Kapv5vZk"

hashtag = input("Enter the hashtag : ")

# TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)


# Twitter Stream Listener
class KafkaPushListener(StreamListener):
    def __init__(self):
        # localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Get Producer that has topic name is Twitter
    # self.producer = self.client.topics[bytes("twitter")].get_producer()

    def on_data(self, data):
        # Producer produces data for consumer
        # Data comes from Twitter
        self.producer.send("twitter_stream_" + hashtag, data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


# Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

hashStr = "#"+ hashtag

# Produce Data that has trump hashtag (Tweets)
twitter_stream.filter(track=[hashStr])