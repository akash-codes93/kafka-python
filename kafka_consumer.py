import sys

from confluent_kafka import Consumer, DeserializingConsumer
from confluent_kafka import KafkaException, KafkaError


deserializer = lambda x, y: str(x)

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "foo1",
        'auto.offset.reset': 'earliest',
        'value.deserializer': deserializer
        }

# consumer1 = Consumer(conf)
consumer1 = DeserializingConsumer(conf)

running = True


def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # msg_process(msg)
                # print(dir(msg))
                print(msg.value())
                print(msg.partition())
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


basic_consume_loop(consumer=consumer1, topics=['first_topic'])
