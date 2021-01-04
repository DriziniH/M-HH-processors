import sys
import ast
import json

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from src.schema import properties as p
from datetime import datetime

from src.schema_conversion import convert_schema
import src.io as io
import src.constants as c

from pyspark.sql.types import *
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# TODO consume from new processed topic

# https://docs.confluent.io/clients-confluent-kafka-python/current/index.html

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "car",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

running = True


def extract_metadata(metadata):
    try:
        region_string = metadata["region"]
        timestamp_millis = metadata["timestamp"]
        car_id = metadata["carID"]

        if region_string not in p.REGIONS:
            raise Exception("Region not available!")

        region = p.REGIONS[region_string]
        dt = datetime.fromtimestamp(timestamp_millis/1000.0)

        return region, timestamp_millis, dt, car_id

    except Exception as e:
        print(e)
        return None


def process_msg(msg):
    key = msg.key().decode('UTF-8')
    data = msg.value().decode('UTF-8')

    metadata = json.loads(key)

    region, timestamp_millis, dt, car_id = extract_metadata(metadata)

    # time data
    day = dt.day
    month = dt.month
    year = dt.year

    # persist raw events
    event = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": key,
        "value": data
    }


def shutdown():
    running = False


def consume_log(consumer, topics):

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
                # print("Topic: %s || Partition:%d || Offset:%d" % (msg.topic(), msg.partition(),
                #                             msg.offset()))
                # print(f'Key: {msg.key()}')
                # print(f'Value: {msg.value()}')
                process_msg(msg)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


consume_log(consumer, ["car-usa"])
