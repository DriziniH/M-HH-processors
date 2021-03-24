import sys
import json
from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utility import dict_tools
from src.utility.logger import logger
# from src.utility.graph_tools import create_json_graph


running = True


def extract_message(msg):
    """Decodes and extracts metadata information from kafka message

    Args:
        msg (kafka message)

    Raises:
        Exception: Failure information while extracting data

    Returns:
        tuple: metadata, data
    """
    try:
        metadata = {}

        if msg.key():
            key = msg.key().decode('UTF-8')
            metadata = dict_tools.load_json_to_dict(key)

        data = dict_tools.load_json_to_dict(msg.value().decode('UTF-8'))

        return metadata, data

    except Exception as e:
        logger.warning(e)
        raise e


def process_analysis_results(msg, mongo_db):
    """Extracts message information and writes results to mongo db

    Args:
        msg (kafka message): analysis results
        mongo_db (db): mongo db instance
    """

    try:
        metadata, analysis_results = extract_message(msg)

        db_col = mongo_db.get_collection("analysis")

        _id = {"_analysisType": metadata["analysisType"]}
        chart_type = analysis_results.get("chartType", "")

        if not chart_type:
            raise Exception("No chart type provided")
        elif chart_type == "log":
            update_data = {
                "data": {str(analysis_results["key"]): analysis_results["value"]}}
            update_statement = {"$push": update_data}
            db_col.update(_id, update_statement, upsert=True)
        else:
            update_statement = {"$set": analysis_results}
            db_col.update(_id, update_statement, upsert=True)

    except Exception as e:
        logger.error(f'Failed to process analysis results: {str(e)}')
        raise e


def shutdown():
    running = False


def consume_log(topics, mongo_db):
    """Infinitly reads kafka log from latest point

    Args:
        topics (String[]): Topics to read from
        mongo_db (db): mongo instance

    Raises:
        KafkaException: Kafka exception
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9093",
            'group.id': "analysis-processor",
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
                process_analysis_results(msg, mongo_db)

    except Exception as e:
        logger.error(f'Error consuming kafka log : {str(e)}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def start_processor(topics, mongo_db):
    """Connects to MongoDB and starts consuming the log from given topics
    Args:
        topics (String[]): kafka topics
        mongo_db (String): MongoDB instance
    """

    consume_log(topics, mongo_db)
