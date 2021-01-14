from src.conf import properties as p
from src.schema_conversion_dict import convert_schema
import src.utility.io as io
import src.conf.constants as c
from src.producer import publish
from src.utility.dict_tools import flatten_json
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

import sys
import ast
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import pandas as pd


running = True

mongo = MongoDB(pm.processed_db_con, "M-HH-car")


def process_data(metadata, data, msg):
    """
    Reads schemas from properties
    Creates mapped data based on all schemas from given region 
    Publishes data to specific topic
    Persist data as parquet in data lake

    Args:
        metadata (dict): message metadata
        data (dict): car data
        data_base (dict) : base data with car id and time information

    """
    region = metadata["region"]
    topic = region[c.TOPICS][c.TOPIC_PROCESSED]

    for key, value in region[c.SCHEMAS].items():

        

        # read schema and convert to struct
        with open(value) as f:
            schema = json.load(f)

        data_base = {
            "timestamp": metadata["timestamp"],
            "schema": key
        }

        mapped_data = convert_schema(schema, data, data_base)

        # Add schema information for processed data
        new_key = {
            "schema": key,
            "carId": metadata["carId"]
        }

        io.write_json_lines(
            f'{region[c.PROCESSED]}year={metadata["datetime"].year}\\month={metadata["datetime"].month}\\day={metadata["datetime"].day}\\{topic}.json', "a", mapped_data)

        publish(topic, key=new_key,
                data=mapped_data)

        # persist processed data to mongodb
        # TODO this should be done by another script reading the processed topic and persisting it to mongodb
        mongo.upsert_to_mongodb(col=mongo.get_collection(
            "usa_car"), _id={"carId": metadata["carId"], "schema": key}, data=mapped_data)


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
        key = msg.key().decode('UTF-8')
        value = msg.value().decode('UTF-8')

        metadata = json.loads(msg.key())

        car_id = metadata["carId"]

        region = extract_region_from_topic(msg.topic())
        if not region:
            raise Exception("Region not available!")

        timestamp_millis = msg.timestamp()[1]
        dt = datetime.fromtimestamp(timestamp_millis/1000)

        metadata["timestamp"] = timestamp_millis
        metadata["datetime"] = dt
        metadata["region"] = region

        return key, value, metadata

    except Exception as e:
        print(e)
        return None


def process_msg(msg):
    """Reads message, persists events, raw data and clean data as dataframe in parquet
    schematizes and publishes data for all schemas

    Args:
        msg (kafka message): kafka message
    """

    key, value, metadata = extract_message(
        msg)

    # time data
    dt = metadata["datetime"]
    day = dt.day
    month = dt.month
    year = dt.year

    # persist raw events
    event = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": msg.timestamp(),
        "key": key,
        "value": value
    }

    region = metadata["region"]
    io.write_data(
        f'{region[c.RAW_EVENTS]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

    data = json.loads(value)

    io.write_json_lines(
        f'{region[c.RAW]}year={year}\\month={month}\\day={day}\\{msg.topic()}.json', "a", data)

    # flatten values for columnar format and schema conversion
    data = flatten_json(data)

    process_data(metadata, data, msg)


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
                process_msg(msg)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_raw_processor():
    consume_log(["car-usa"])
