from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': 'my-new-group',
        'auto.offset.reset': 'earliest',
        }

# consumer1 = Consumer(conf)
consumer = Consumer(conf)

topic = 'first_topic'

# creating a topic partition with topic - 'first_topic' and partition - 0
topicPartition = TopicPartition(topic=topic, partition=2)
print(topicPartition)

consumer.assign([topicPartition])

topicPartition.offset = OFFSET_BEGINNING
consumer.seek(topicPartition)

while True:
    message = consumer.poll(timeout=1.0)
    print(message.code())
    print(message.value())
