from threading import Thread
from datetime import datetime, timedelta

from src.utility.logger import logger
from src.analysis_processor import start_processor
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

mongo_db = MongoDB(pm.db_con, "M-HH")
units = mongo_db.get_collection("config").find({})



Thread(target=start_processor, args=(["global-analysis"], mongo_db)).start()
logger.info(
    f'Started analysis processor thread for global analysis. Consuming Topic: global-analysis')
