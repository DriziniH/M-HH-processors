from threading import Thread
from datetime import datetime, timedelta

from src.utility.logger import logger
from src.processor import start_processor
from src import constants as c
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB


mongo_db = MongoDB(pm.db_con, "M-HH")

units = mongo_db.get_collection("units").find({})


for unit in units:
    for topic_type, topic in unit["topics"].items():
        Thread(target=start_processor, args=(
            unit, [topic], mongo_db, topic_type)).start()
        logger.info(
            f'Started {topic_type} processor thread for {unit["label"]}. Consuming Topic: {topic}')


### Write json to parquet -> interval job (daily)
# path_json = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR\\"
# file_json = "car-usa.json"
# path_parquet = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR_PARQUET\\"


# write_json_to_partioned_parquet(
#     path_json, file_json, path_parquet, datetime.now() - timedelta(1))