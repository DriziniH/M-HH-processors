from threading import Thread
from datetime import datetime, timedelta

from src.utility.logger import logger
from src.processor import start_processor
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB
from src.write_produced_schema_to_mongo import write_produced_schema_to_mongo

mongo_db = MongoDB(pm.db_con, "M-HH")
units = mongo_db.get_collection("units").find({})


for unit in units:
    for topic_type, topic in unit["topics"].items():
        Thread(target=start_processor, args=(
            unit, [topic], mongo_db, topic_type)).start()
        logger.info(
            f'Started {topic_type} processor thread for {unit["label"]}. Consuming Topic: {topic}')


# Extract schema produced in given region (topic) and writes it to mongodb
# write_produced_schema_to_mongo("car-usa", "f590cd1c-daec-4686-b9af-0bb831f9d5bc")
