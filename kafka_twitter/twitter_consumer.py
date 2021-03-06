import sys
import json

from confluent_kafka import Consumer, KafkaError, KafkaException
from config import *
from elasticsearch_client import ClsElasticSearch


class ClsTwitterConsumer:

    _config = {}

    def __init__(self):
        self.get_or_update_config()
        self.consumer = self.get_consumer()
        self.consumer.subscribe([self.__class__._config['TOPIC']])
        self.elasticSearch = ClsElasticSearch(self.__class__._config['BONSAI_URL'])

    def get_or_update_config(self):
        self.__class__._config.update({
            'BOOTSTRAP_SERVER': BOOTSTRAP_SERVER,
            'TOPIC': TOPIC,
            'BONSAI_URL': BONSAI_URL,
        })

    def get_kafka_properties(self):
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        return {
            'bootstrap.servers': self.__class__._config['BOOTSTRAP_SERVER'],
            'group.id': 'example1',
            'auto.offset.reset': 'beginning',
            # 'auto.offset.reset': 'earliest',
            'session.timeout.ms': 6000,
            'enable.auto.commit': False
           }

    def get_consumer(self):
        return Consumer(**self.get_kafka_properties())

    def get_tweets(self):

        while True:
            tweet = self.consumer.poll(timeout=1.0)
            if tweet is None:
                continue

            if tweet.error():
                if tweet.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (tweet.topic(), tweet.partition(), tweet.offset()))
                elif tweet.error():
                    raise KafkaException(tweet.error())
            else:
                yield tweet

                self.consumer.commit()

    def prepare_tweet(self):
        for tweet in self.get_tweets():
            tweet = json.loads(tweet.value())
            try:
                doc = {
                    "_id": tweet["id"],
                    "text": tweet["text"]
                }
            except KeyError:
                print("skipping tweet: ", tweet)
            else:
                yield doc

    def close(self):
        self.consumer.close()

    def post_to_elastic_search(self):
        count = 0
        try:
            for tweet in self.get_tweets():
                try:
                    # this is a unique id to every tweet present in broker; we can use this also instead of tweet id
                    unique_id = tweet.topic() + '_' + str(tweet.partition()) + '_' + str(tweet.offset())
                    print(unique_id)

                    tweet = json.loads(tweet.value())
                    self.elasticSearch.insert("twitter", {"id": tweet["id"], "text": tweet["text"]})

                    print("Tweets inserted: {0}, latest tweet: {1}".format(str(count), tweet["text"]))
                    count += 1

                except KeyError:
                    print("skipping tweet: ", tweet)

        except Exception as e:
            print("Error inserting in elastic search: {error}".format(error=str(e)))

        finally:
            self.elasticSearch.close()

    def post_to_elastic_search_bulk(self):
        try:
            self.elasticSearch.bulk_insert("twitter", self.prepare_tweet)
        except Exception as e:
            print("Error inserting in elastic search: {error}".format(error=str(e)))
        finally:
            self.elasticSearch.close()


if __name__ == '__main__':

    twitterConsumer = ClsTwitterConsumer()
    twitterConsumer.post_to_elastic_search_bulk()
    twitterConsumer.close()
