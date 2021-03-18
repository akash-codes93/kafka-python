from kafka import KafkaProducer

bootstrap_servers = 'localhost:9092'
# connect to kafka
# https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# we can only send bytes data to kafka
string_to_bytes = lambda x: bytes(x, 'utf-8')

# sending data, this is async
producer.send(topic='first_topic', value=string_to_bytes("this is via python code"))
producer.send(topic='first_topic', value=string_to_bytes("awesome!!"))

# flush and close
producer.flush()
producer.close()

# -- using confluent client --

from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "host1:9092,host2:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)
