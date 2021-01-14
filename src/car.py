import sys
from threading import Thread
import uuid
import random
import json
import ast
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

from src import producer
from src import consumer
from src.conf import properties_showcase as ps


running = True


def shutdown():
    running = False


def process_msg(msg, car_id):
    try:
        key = msg.key().decode('UTF-8')
        if car_id == key:
            data = msg.value().decode('UTF-8')
            if data:
                # TODO PROCESS
                # print(f'ID: {car_id}')
                # print(f'Info: {data}')
                pass
    except Exception as e:
        pass  # print(e)


def consume_log(topics, car_id):
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
                process_msg(msg, car_id)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        print("Thread stopped")


def start_car(amount, region, topic_produce, topics_consume):

    for i in range(amount):
        try:
            car_id = ps.IDS[i]
        except Exception as e:
            car_id = str(uuid.uuid4())

        key = {"region": region, "id": car_id, "origin": "car"}

        if region == "EU":
            data = ps.get_car_data_eu()
        elif region == "USA":
            data = ps.get_car_data_usa()

        # Producer
        Thread(target=producer.publish_infite, args=(
            topic_produce, key, data)).start()

        # Consumer
        Thread(target=consume_log, args=(topics_consume, car_id)).start()
