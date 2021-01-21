import sys
import ast
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

from src.utility.logger import logger

running = True

def shutdown():
    running = False

def print_message(msg, data):
    logger.info("Topic: %s || Partition:%d || Offset:%d" % (msg.topic(), msg.partition(),
                                msg.offset()))
    logger.info(f'Key: {msg.key()}')
    logger.info(f'Value: {msg.value()}')
    logger.info(f'Data: {data}' )
    


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
                    logger.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.info(msg.key())
                logger.info(msg.value())
    except Exception as e:
        logger.error(f'Error consuming kafka log : {str(e)}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


