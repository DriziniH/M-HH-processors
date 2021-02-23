from src.utility.logger import logger
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB
from src.utility import dict_tools
from confluent_kafka import Consumer


def write_produced_schema_to_mongo(topic, unit_id):
    mongo_db = MongoDB(pm.db_con, "M-HH")
    col = mongo_db.get_collection("units")

    conf = {'bootstrap.servers': "localhost:9092",
            'group.id': "car",
            'auto.offset.reset': 'smallest'}

    consumer = Consumer(conf)
    processed_topics = []

    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            data = dict_tools.load_json_to_dict(msg.value())
            schema = dict_tools.extract_schema("data-eu", data)
            col.update_one({"_id": unit_id}, {
                           "$set": {"producedData": schema["fields"]}})
            break

    except Exception as e:
        logger.error(f'Error consuming kafka log : {str(e)}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
