import sys
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

import src.utility.io as io
import src.constants as c
from src.utility import dict_tools
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


def process_data(processor, metadata, data, msg, dt):
    """Writes processed data to data lake and updates mongodb

    Args:
        processor (dict): objects of processor
        metadata (dict)
        data (dict)
        timestamp (int)
        dt (Datetime)
    """

    try:
        data["timestamp"] = msg.timestamp()[1]
        data["origin"] = metadata.get("origin", msg.topic())
        data["id"] = metadata["id"]

        # persist to data lake
        dl_path = processor["conf"]["pathsDL"]["processed"]
        io.write_json_lines(
            f'{dl_path}year={dt.year}\\month={dt.month}\\day={dt.day}\\{msg.topic()}.json', "a", data)

        # persist to db
        mongo_db = processor["mongo_db"]
        db_col = mongo_db.get_collection(
            "processed")

        _id = {
            "_id": data["id"],
            "_origin":  data["origin"]
        }

        mongo_db.upsert_to_mongodb(
            col=db_col, _id=_id, data=data, mode="$set")

    except Exception as e:
        logger.error(f'Failed to process data: {str(e)}')
        return


def process_analysis_results(processor, metadata, analysis_results, msg, dt):
    """Writes analysis results to data lake and updates mongodb

    Args:
        processor (dict): objects of processor
        metadata (dict)
        data (dict)
        timestamp (int)
        dt (Datetime)
    """

    try:
        if "id" in metadata:
            analysis_results.update({"id": metadata.get("id", "")})

        analysis_results.update({"origin": metadata.get("origin", msg.topic()),
                                 "timestamp": msg.timestamp()[1]})

        # persist to data lake
        dl_path = processor["conf"]["pathsDL"]["analyzed"]
        io.write_json_lines(
            f'{dl_path}year={dt.year}\\month={dt.month}\\day={dt.day}\\{msg.topic()}.json', "a", analysis_results)

        # dont write car analysis data to db
        if msg.topic() in processor["conf"]["topics"]["analysisCar"]:
            return

        # persist to db
        mongo_db = processor["mongo_db"]
        db_col = mongo_db.get_collection("analysis")

        _id = {"type": metadata["type"],
               "region": metadata["region"]}

        json_graph = analysis_results["jsonGraph"]
        # update fields with $set operator is values are lists
        if type(json_graph["x"]) is list and type(json_graph["y"]) is list:
            mongo_db.upsert_to_mongodb(
                col=db_col, _id=_id, data=analysis_results, mode="$set")
        else:  # update fields and update append arrays
            values = {"jsonGraph": {
                "x":  analysis_results["jsonGraph"].pop("x"), "y": analysis_results["jsonGraph"].pop("y")}}

            mongo_db.upsert_to_mongodb(
                col=db_col, _id=_id, data=analysis_results, mode="$set")
            mongo_db.upsert_to_mongodb(
                col=db_col, _id=_id, data=values, mode="$push")

    except Exception as e:
        logger.error(f'Failed to process analysis results: {str(e)}')
        raise e


def process_msg(msg, processor):
    """
    Extracts and decodes message
    Persists raw event 
    Persists decoded and enriched data as json
    Writes data to MongoDB

    Args:
        msg (kafka message): kafka message
    """
    try:
        key, value, metadata = extract_message(
            msg)

        # time data
        dt = metadata.get("datetime", datetime.now())
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
            f'{processor["conf"]["pathsDL"]["rawEvents"]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

        data = dict_tools.load_json_to_dict(value)

        proc_type = processor.get("proc_type", None)
        if proc_type == "ingest":
            process_data(processor, metadata, data, msg, dt)
        elif "analysis" in proc_type:
            process_analysis_results(processor, metadata, data, msg, dt)
    except Exception as e:
        logger.error(f'Failed to process message: {str(e)}')
        return


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
        logger.error(f'Error consuming kafka log : {str(e)}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_processor(conf, topics, mongo_db, processor_type="ingest"):
    """Connects to MongoDB and starts consuming the log from given topics
    Args:
        conf (dict): region config
        topics (String[]): kafka topics
        dbcon (String): MongoDB connection string
    """

    processor = {
        "conf": conf,
        "proc_type": processor_type,
        "mongo_db": mongo_db
    }

    consume_log(topics, processor)
