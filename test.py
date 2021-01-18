import json
from src.utility import io
from src.conf import properties_showcase as ps
import logging
from src.utility.logger import logger
import uuid
from datetime import datetime
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

for _ in range(4):
    print(uuid.uuid4())



