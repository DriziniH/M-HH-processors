from threading import Thread

from src.raw_processor import start_raw_processor
from src.analysis_processor import start_analysis_processor

Thread(target=start_raw_processor, args=()).start()
Thread(target=start_analysis_processor, args=()).start()