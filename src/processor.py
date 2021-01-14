import src.utility.io as io
import src.conf.constants as c
from src.utility import dict_tools
from src.utility.mongo_db import MongoDB

import sys
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import pandas as pd

running = True


def extract_message(msg):
    """Decodes and extracts metadata information from kafka message

    Args:
        msg (kafka message)

    Raises:
        Exception: Failure information while extracting data

    Returns:
        tuple: key, value, metadata
    """
    try:
        key = msg.key().decode('UTF-8')
        value = msg.value().decode('UTF-8')

        metadata = dict_tools.load_json_to_dict(msg.key())

        timestamp_millis = msg.timestamp()[1]
        dt = datetime.fromtimestamp(timestamp_millis/1000)

        metadata["timestamp"] = timestamp_millis
        metadata["datetime"] = dt

        return key, value, metadata

    except Exception as e:
        print(e)
        return None


def process_msg(msg, processor):
    """
    Extracts and decodes message
    Persists raw event 
    Persists decoded and enriched data as json
    Writes data to MongoDB

    Args:
        msg (kafka message): kafka message
    """

    region_conf = processor["conf"]
    proc_type = processor["proc_type"]
    mongo_db = processor["mongo_db"]

    dl_paths = region_conf[c.DL_PATHS]
    topics = region_conf[c.TOPICS]

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
        "key": "" if key is None else key,
        "value": value
    }

    io.write_data(
        f'{dl_paths[c.RAW_EVENTS]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

    data = dict_tools.load_json_to_dict(value)
    data["id"] = metadata["_id"]  # TODO "id"
    data["origin"] = metadata["origin"]
    data["timestamp"] = msg.timestamp()[1]

    if proc_type == "raw":
        path = dl_paths[c.PROCESSED]
    elif proc_type == "analysis":
        path = dl_paths[c.ANALYZED]

    io.write_json_lines(
        f'{dl_paths[c.PROCESSED]}year={year}\\month={month}\\day={day}\\{msg.topic()}.json', "a", data)

    # dont write car analysis data
    if msg.topic() != topics[c.TOPIC_INFO_CAR]:

        db_cols = region_conf[c.DB_COLS]

        if proc_type == "raw":

            _id = {
                "_id": metadata["_id"], #TODO "id"
                "_origin":  metadata["origin"]
            }

            mongo_db.upsert_to_mongodb(col=mongo_db.get_collection(
                db_cols[c.PROCESSED]), _id=_id, data=data)

        elif proc_type == "analysis":

            mongo_db.upsert_to_mongodb(col=mongo_db.get_collection(
                db_cols[c.PROCESSED]), _id={metadata["info_type"]}, data={"data": data}, mode="$push")


def shutdown():
    running = False


def consume_log(topics, processor):
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
                process_msg(msg, processor)
    except Exception as e:
        print(e)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_processor(conf, topics, dbcon, processor_type="raw"):
    """Connects to MongoDB and starts consuming the log from given topics
    Args:
        conf (dict): region config
        topics (String[]): kafka topics
        dbcon (String): MongoDB connection string
    """

    mongo_db = MongoDB(dbcon, conf[c.DB_NAME])

    processor = {
        "conf": conf,
        "proc_type": processor_type,
        "mongo_db": mongo_db
    }

    consume_log(topics, processor)
