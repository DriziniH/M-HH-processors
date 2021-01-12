import sys
import ast
import json

from src.conf import properties as p

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

running = True

def shutdown():
    running = False

def print_message(msg, data):
    print("Topic: %s || Partition:%d || Offset:%d" % (msg.topic(), msg.partition(),
                                msg.offset()))
    print(f'Key: {msg.key()}')
    print(f'Value: {msg.value()}')
    print(f'Data: {data}' )
    


def consume_log(topics):
    """Infinitly reads kafka log from latest point

    Args:
        topics (String[]): Topics to read from

    Raises:
        KafkaException: Kafka exception
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'group.id': "car",
            'auto.offset.reset': 'smallest'}

    consumer = Consumer(conf)

    try:
        consumer.subscribe([topics])

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
                print(msg.key())
                print(msg.value())

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


