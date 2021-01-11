from confluent_kafka import Producer
from datetime import datetime
import socket
import base64
import json
import random
import uuid
import time
import random

from src.conf import properties_showcase as ps


running = True


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        running = False
    else:
        # print("Message produced: %s || %s || %s" %
        #       (msg.topic(), msg.key(), msg.value()))
        pass


def publish_infite(topic, car_id, region):
    """Publishes infinitly on given topic with random generated key and values

    Args:
        topic (String): Topic to publish to
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}

    producer = Producer(conf)

    key = {"region": region, "carId": car_id}

    if region == "EU":
        data = ps.get_car_data_eu()
    elif region == "USA":
        data = ps.get_car_data_usa()


    while running:

        timestamp = int(datetime.now().timestamp() * 1000)

        producer.produce(topic, key=json.dumps(key).encode(), timestamp=timestamp,
                         value=json.dumps(data).encode(), callback=acked)

        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method calls if the message is acknowledged.
        producer.poll(1)

        time.sleep(1)


def publish(topic, key, data):
    """Publishes one time on given topic with key and data

    Args:
        topic (String): Topic to publish to
    """

    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}

    producer = Producer(conf)

    timestamp = int(datetime.now().timestamp() * 1000)

    key = json.dumps(key).encode()
    data = str(data).encode()

    producer.produce(topic, key=key, timestamp=timestamp,
                     value=data, callback=acked)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(1)


def shutdown():
    running = False
