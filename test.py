import json
from src.utility import io
from src.conf import properties_showcase as ps
import logging
from src.utility.logger import logger
import uuid
from datetime import datetime
from json_to_parquet import write_json_to_partioned_parquet
from datetime import datetime, timedelta

dt = datetime.now()

path_json = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR\\"
path_parquet = f"C:\\Showcase\\Projekt\\M-HH-showcase-local\\data-lake\\S3_USA_PROCESSED\\CAR_PARQUET\\"

write_json_to_partioned_parquet(path_json,"car-usa.json",path_parquet,datetime.now() - timedelta(1))

