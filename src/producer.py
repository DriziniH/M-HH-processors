from confluent_kafka import Producer
from datetime import datetime
import socket
import base64
import json
import random
import uuid
import time


running = True


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        running = False
    else:
        print("Message produced: %s || %s" % (msg.key(), msg.value()))


def publish_infite(topic, car_id):
    """Publishes infinitly on given topic with random generated key and values

    Args:
        topic (String): Topic to publish to
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}

    producer = Producer(conf)

    key = {"region": "USA", "carID": car_id}

    while running:

        timestamp = datetime.now().timestamp() * 1000
        key["timestamp"] = timestamp

        data = f'SENS_TEMP_ENGINE: {random.uniform(30.0,70.0)}, SENS_TEMP_BREAKS:  {random.uniform(30.0,70.0)}, SENS_TEMP_TIRES: {random.uniform(30.0,120.0)}, SENS_OIL:{random.randrange(50, 100)}, SENS_BREAK: {bool(random.getrandbits(1))}, SENS_GAS:{random.randrange(80, 120)}, SENS_MPH:{random.uniform(0,200)}, SENS_LAT:{random.uniform(80.0,83.0)}, SENS_LON: {random.uniform(90.0,99.0)}'

        producer.produce(topic, key=json.dumps(key).encode(),
                        value=data.encode(), callback=acked)

        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
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
    
    timestamp = datetime.now().timestamp() * 1000
    key["timestamp"] = timestamp

    key = json.dumps(key).encode()
    data = str(data).encode()
    
    producer.produce(topic, key=key,
                    value=data, callback=acked)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(1)
    

def shutdown():
    running = False