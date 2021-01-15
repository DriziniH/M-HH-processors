import sys
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

import src.utility.io as io
import src.constants as c
from src.utility import dict_tools
from src.utility.mongo_db import MongoDB
from src.utility.logger import logger


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
        logger.warning(e)
        return None


def get_params_from_processor_type(processor, metadata, data, msg):
    """Takes information about current processor (raw or analysis) and returns tuple with information and data

    Args:
        processor (dict): objects of processor
        metadata (dict)
        data (dict)
        timestamp (int):

    Returns:
        data[dict]: enriched data
        _id[dict]: if fields for mongo db update
        dl_path[String]: Data lake path where data is persisted
        db_col[MongoDB Collection]: Collection where data is updated
        mode[String]: Mode how data is updated to MongoDB
    """

    paths = processor["conf"][c.DL_PATHS]
    db_cols = processor["conf"][c.DB_COLS]
    topics = processor["conf"][c.TOPICS]
    mongo_db = processor["mongo_db"]

    data["timestamp"] = msg.timestamp()[1]

    if processor["proc_type"] == "raw":
        try:
            data["origin"] = metadata["origin"]
            data["id"] = metadata["id"]
        except:
            data["id"] = metadata["_id"]  # TODO REMOVE Try catch block

        _id = {
            "_id": data["id"],
            "_origin":  data["origin"]
        }
        dl_path = paths[c.PROCESSED]
        db_col = mongo_db.get_collection(db_cols[c.PROCESSED])
        mode = "$set"

    elif processor["proc_type"] == "analysis":
        _id = {"type": metadata["type"]}
        db_col = mongo_db.get_collection(db_cols[c.ANALYZED_REGION])
        mode = "$push"

        if msg.topic() == topics[c.TOPIC_INFO_CAR]:
            dl_path = paths[c.ANALYZED_CAR]
        elif msg.topic() == topics[c.TOPIC_INFO_REGION]:
            dl_path = paths[c.ANALYZED_REGION]

    return data, _id, dl_path, db_col, mode


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

    data, _id, dl_path, db_col, mode = get_params_from_processor_type(
        processor, metadata, data, msg)

    io.write_json_lines(
        f'{dl_path}year={year}\\month={month}\\day={day}\\{msg.topic()}.json', "a", data)

    if msg.topic() != topics[c.TOPIC_INFO_CAR]:  # dont write car analysis data

        if proc_type == "analysis":
            data = {"data": data}

        mongo_db.upsert_to_mongodb(col=db_col, _id=_id, data=data, mode=mode)


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
        logger.error(f'Error consuming kafka log : {e}')
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
