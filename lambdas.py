from src.processor import start_processor
from src.conf import properties_eu, properties_usa
from src import constants as c
from src.conf import properties_mongo as pm

from threading import Thread


conf_eu = properties_eu.CONF
conf_usa = properties_usa.CONF

Thread(target=start_processor, args=(
    conf_eu, [conf_eu[c.TOPICS][c.TOPIC_RAW]], pm.db_con_eu,"raw")).start()

Thread(target=start_processor, args=(
    conf_usa, [conf_usa[c.TOPICS][c.TOPIC_RAW]], pm.db_con_usa,"raw")).start()
Thread(target=start_processor, args=(
    conf_usa, [conf_usa[c.TOPICS][c.TOPIC_INFO_REGION],conf_usa[c.TOPICS][c.TOPIC_INFO_CAR]], pm.db_con_usa,"analysis")).start()
