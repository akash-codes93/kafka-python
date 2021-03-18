# -- using confluent client --

from confluent_kafka import Producer

conf = {'bootstrap.servers': "localhost:9092"}

producer = Producer(conf)


def ack(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        # print(dir(msg))
        # print(msg.headers())
        # print(msg.key())
        # print(msg.latency())
        # print(msg.offset())
        print("key: " + str(msg.key()) + ", Partition: " + str(msg.partition()))
        # print("Partition: ", msg.partition())
        # print(msg.timestamp())
        # print(msg.topic())
        # print(msg.value())
        # print("Message produced: %s" % (str(msg)))


# keeping on producing messages
for i in range(0, 10):
    # partition with same key goes to the same partition
    key = 'id_' + str(i % 3)
    producer.produce('first_topic', key=key, value="code cannot run", callback=ack)

# Wait up to 1 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
    producer.poll(1)
# producer.flush()
