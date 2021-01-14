from threading import Thread

from src.raw_processor import start_raw_processor
from src.analysis_processor import start_analysis_processor
from src.conf import properties_eu, properties_usa



Thread(target=start_raw_processor, args=(properties_eu.CONF)).start()
Thread(target=start_raw_processor, args=(properties_usa.CONF)).start()

Thread(target=start_analysis_processor, args=(properties_usa.conf)).start()
