import json
from src.utility import io
from src.conf import properties_showcase as ps
import logging
from src.utility.logger import logger
import uuid
from datetime import datetime
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB
from src.utility import dict_tools


# mongo_db = MongoDB(pm.db_con, "M-HH")

# conf_col = mongo_db.get_collection("config")

# conf = conf_col.find_one({}, {'_id': False})

print(uuid.uuid4())
