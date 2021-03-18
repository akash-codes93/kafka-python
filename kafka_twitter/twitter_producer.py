import atexit
import json

from confluent_kafka import Producer
from config import *
from twitter_client import ClsGetStream


class ClsTwitterProducer:

    _config = {}

    def __init__(self):
        self.get_or_update_config()
        self.count = 0

    def get_or_update_config(self):

        self.__class__._config.update({
            'BOOTSTRAP_SERVER': BOOTSTRAP_SERVER,
            'TOPIC': TOPIC,
            'BEARER_TOKEN': BEARER_TOKEN,
            'SAMPLE_STREAM_URL': SAMPLE_STREAM_URL
        })

    def get_twitter_data(self):

        obj = ClsGetStream(
            self.__class__._config['SAMPLE_STREAM_URL'],
            self.__class__._config['BEARER_TOKEN']
        )

        tweets = obj.get_data()

        for tweet in tweets:
            try:
                # we want id and text
                yield tweet["data"]
            except KeyError:
                print("Data read error, value: " + tweet)
            except TypeError:
                print('Data type error: ', tweet)

    def get_kafka_properties(self):
        return {
            'bootstrap.servers': self.__class__._config['BOOTSTRAP_SERVER'],
            'enable.idempotence': True,
            # should be 2 but since only one broker is present we have set it to 1
            'request.required.acks': 1,
            # 'compression.type': 'snappy',
            'acks': 'all',
            'linger.ms': 20,
            # unit in Bytes defaults 16KB
            'batch.size': 32768
           }

    def get_producer(self):
        return Producer(**self.get_kafka_properties())

    def acknowledge_write(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            self.count += 1
            print("Total tweets written: " + str(self.count) + ", Partition: " + str(msg.partition()))

    def write_kafka(self):
        producer = self.get_producer()

        for tweet in self.get_twitter_data():
            if self.count > 50:
                producer.poll(1)
                break
            producer.produce(
                self.__class__._config['TOPIC'],
                value=json.dumps(tweet), callback=self.acknowledge_write
            )

            producer.poll(1)


def on_exit(): print("The application is closing!")


atexit.register(on_exit)


if __name__ == '__main__':

    twitter_producer = ClsTwitterProducer()
    twitter_producer.write_kafka()

# verge
# ardor
# argon
# tron
# internal ops tool requirement ok sure.
