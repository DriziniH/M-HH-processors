from time import sleep
from json import dumps

from confluent_kafka import Producer

from datetime import datetime
import socket

import uuid
import base64

import json


# https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)


data_b = b'SENS_TEMP_ENGINE: 70.0, SENS_TEMP_BREAKS:  60.4, SENS_TEMP_TIRES: 120.4, SENS_OIL:77, SENS_BREAK: true, SENS_GAS:130, SENS_MPH:132.0, SENS_LAT:87.0, SENS_LON: 99.2'
key = {"region": "USA", "carID": str(uuid.uuid4())}


running = True


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        running = False
    else:
        print("Message produced: %s || %s" % (msg.key(), msg.value()))


while running:
    timestamp = datetime.now().timestamp() * 1000
    key["timestamp"] = timestamp

    producer.produce('car-usa', key=json.dumps(key).encode(),
                     value=data_b, callback=acked)

    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    producer.poll(1)
