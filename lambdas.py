from threading import Thread

from src.utility.logger import logger
from src.processor import start_processor
from src.conf import properties_eu, properties_usa
from src import constants as c
from src.conf import properties_mongo as pm


conf_eu = properties_eu.CONF
conf_usa = properties_usa.CONF

Thread(target=start_processor, args=(
    conf_eu, [conf_eu[c.TOPICS][c.TOPIC_INGEST]], pm.db_con_eu,"ingest")).start()
logger.info(f'Started ingest processor thread for EU region.')

Thread(target=start_processor, args=(
    conf_usa, [conf_usa[c.TOPICS][c.TOPIC_INGEST]], pm.db_con_usa,"ingest")).start()
logger.info(f'Started ingest processor thread for USA region.')

Thread(target=start_processor, args=(
    conf_usa, [conf_usa[c.TOPICS][c.TOPIC_ANALYSIS_REGION],conf_usa[c.TOPICS][c.TOPIC_ANALYSIS_CAR]], pm.db_con_usa,"analysis")).start()
logger.info(f'Started analysis processor thread for USA region.')
