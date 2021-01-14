from src.conf import properties as p
import src.utility.io as io
import src.conf.constants as c
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

import sys
import ast
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


running = True
mongo = MongoDB(pm.analysis_db_con, "M-HH-analysis")


def extract_region_from_topic(topic):
    for region, dic in p.REGIONS.items():
        for _, value in dic[c.TOPICS].items():
            if topic == value:
                return dic
    return {}


def extract_message(msg):
    """Extracts metadata information from message

    Args:
        metadata (dict): metadata from message

    Raises:
        Exception: Failure information while extracting data

    Returns:
        tuple: metadata
    """
    try:
        value = msg.value().decode('UTF-8')

        region = extract_region_from_topic(msg.topic())
        if not region:
            raise Exception("Region not available!")

        timestamp_millis = msg.timestamp()[1]
        dt = datetime.fromtimestamp(timestamp_millis/1000)

        return value, region, timestamp_millis, dt

    except Exception as e:
        print(e)
        return None


def process_msg(msg, upsert):
    """Reads message, persists events, raw data and clean data as dataframe in parquet
    schematizes and publishes data for all schemas

    Args:
        msg (kafka message): kafka message
    """

    value, region, timestamp_millis, dt = extract_message(
        msg)

    # time data
    day = dt.day
    month = dt.month
    year = dt.year

    # persist raw events
    event = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": msg.timestamp(),
        "value": value
    }
    io.write_data(
        f'{region[c.RAW_EVENTS]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

    data = json.loads(value)
    data["timestamp"] = msg.timestamp()[1]

    # persist analyzed data
    io.write_json_lines(
        f'{region[c.ANALYZED]}year={year}\\month={month}\\day={day}\\{msg.topic()}.json', "a", data)

    if upsert:
        mongo.upsert_to_mongodb(col=mongo.get_collection(
            "usa_analysis"), _id = "type", data=data)


def shutdown():
    running = False


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
                if "region" in msg.topic():
                    process_msg(msg, True)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_analysis_processor():
    consume_log(["region-usa-info", "car-usa-info"])
