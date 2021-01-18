from threading import Thread

from src.utility.logger import logger
from src.processor import start_processor
from src.conf import properties_eu, properties_usa
from src import constants as c
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB


mongo_db = MongoDB(pm.db_con_usa, "M-HH")

conf_col = mongo_db.get_collection("config")

conf = conf_col.find_one({}, {'_id': False})


for _, unit in conf["units"].items():
    for name, topic in unit["topics"].items():

        #If analysis processor with car results
        if name == "analysisCar":
            conf["car"] = True
        else:
            conf["car"] = False

        conf["origin"] = unit["id"]
        Thread(target=start_processor, args=(
            conf, [topic], mongo_db, name)).start()
        logger.info(f'Started {name} processor thread for {unit["label"]}. Consuming Topic: {topic}')

