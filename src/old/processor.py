import sys
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

import src.utility.io as io
from src.utility import dict_tools
from src.utility.logger import logger
from src.utility.graph_tools import create_json_graph


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
        metadata = {}

        if msg.key():
            key = msg.key().decode('UTF-8')
            metadata = dict_tools.load_json_to_dict(key)
            metadata["key"] = key

        data = dict_tools.load_json_to_dict(msg.value().decode('UTF-8'))

        timestamp_millis = msg.timestamp()[1]
        dt = datetime.fromtimestamp(timestamp_millis/1000)

        metadata["timestamp"] = timestamp_millis
        metadata["datetime"] = dt

        return metadata, data

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
        # persist to data lake
        io.write_json_lines(
            f'data-lake/{processor["conf"]["pathsDL"]["landing"]}json/year={dt.year}\\month={dt.month}\\day={dt.day}\\{msg.topic()}.json', "a", data)

        # persist to db
        mongo_db = processor["mongo_db"]
        db_col = mongo_db.get_collection(
            "processed")

        _id = {
            "_id": data.pop("id"),
            "_unit":  metadata["unit"]
        }

        mongo_db.update_to_mongodb(
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
        # persist to data lake
        io.write_json_lines(
            f'data-lake/{processor["conf"]["pathsDL"]["analytics"]}flink/json/year={dt.year}\\month={dt.month}\\day={dt.day}\\{msg.topic()}.json', "a", analysis_results)

        # dont write car analysis data to db
        if msg.topic() == processor["conf"]["topics"]["analysisCar"]:
            return

        # persist to db
        mongo_db = processor["mongo_db"]
        db_col = mongo_db.get_collection("analysis")

        _id = {"_id": metadata["unit"]}

        json_graph = analysis_results["jsonGraph"]

        # Push x and y for appending values
        if not type(json_graph["x"]) is list and not type(json_graph["y"]) is list:
            mongo_db.update_to_mongodb(
                col=db_col, _id=_id, data={"x": json_graph["x"], "y": json_graph["y"]}, mode="$push")

            # Read complete x and y for graph
            result = db_col.find_one(_id)
            json_graph["x"] = result["x"]
            json_graph["y"] = result["y"]

        # Upsert label of region
        mongo_db.update_to_mongodb(
            col=db_col, _id=_id, data={"label": processor["conf"]["label"]}, mode="$set")

        title = json_graph.get("layout" , "").get("title", "")
        chart_type = json_graph.get("type","")

        # Upsert Array entry
        data = {"graphs.$.type": metadata["type"],
                "graphs.$.title": title,
                "graphs.$.jsonGraph":  create_json_graph(json_graph)}
        _id.update({"graphs.type": metadata["type"]})

        if not mongo_db.update_to_mongodb(
                col=db_col, _id=_id, data=data, mode="$set", upsert=False):

            _id.pop("graphs.type")
            data = {
                "type": metadata["type"],
                "title": title,
                "chartType": chart_type,
                "jsonGraph": create_json_graph(json_graph)}

            db_col.update_one(_id, {'$push':  {'graphs': data}}, upsert=True)

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
        metadata, data = extract_message(msg)

        metadata["unit"] = processor["conf"]["_id"]

        # time data
        dt = metadata.get("datetime", datetime.now())
        day = dt.day
        month = dt.month
        year = dt.year

        # persist events #TODO replace by firehose
        event = {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "timestamp": msg.timestamp(),
            "value": data
        }
        if "key" in metadata:
            event["key"] = dict_tools.load_json_to_dict(metadata["key"])

        io.write_data(
            f'data-lake/{processor["conf"]["pathsDL"]["events"]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

        data["timestamp"] = msg.timestamp()[1]

        proc_type = processor.get("proc_type", None)
        if proc_type == "ingest":
            process_data(processor, metadata, data, msg, dt)
        elif "analysis" in proc_type:
            process_analysis_results(processor, metadata, data, msg, dt)
        else:
            raise Exception("Invalid processor type")

    except Exception as e:
        logger.error(f'Failed to process message: {str(e)}')
        return


def shutdown():
    running = False


def consume_log(topics, processor):
    """Infinitly reads kafka log from latest point

    Args:
        topics (String[]): Topics to read from
        processor (dict): Processor variabels
        region_conf (dict): region name and id

    Raises:
        KafkaException: Kafka exception
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'group.id': "car",
            'auto.offset.reset': 'latest'}  # TODO smallest

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
                    logger.error('%% %s [%d] reached end at offset %d\n' %
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
