from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import json
from textblob import TextBlob

es = Elasticsearch(hosts=['localhost'], port=9200)


def main():

    """
    main function initiates a kafka consumer, initialize the tweet database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into postgres database
    """

    hashtag = input("Enter the hashtag : ")
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter_stream_" + hashtag, auto_offset_reset='earliest')

    for msg in consumer:
        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"])
        polarity = tweet.sentiment.polarity
        tweet_sentiment = ""
        if polarity > 0:
            tweet_sentiment = 'positive'
        elif polarity < 0:
            tweet_sentiment = 'negative'
        elif polarity == 0:
            tweet_sentiment = 'neutral'

        # add text & sentiment to es
        es.index(
                    index="tweet_es_" + hashtag + "_index",
                    doc_type="test_doc",
                    body={
                    "author": dict_data["user"]["screen_name"],
                    "date": dict_data["created_at"],
                    "message": dict_data["text"],
                    "sentiment": tweet_sentiment
                    }
                )
        print(str(tweet))
        print('\n')


if __name__ == "__main__":
    main()
