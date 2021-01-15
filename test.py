import json
from src.utility import io
from src.conf import properties_showcase as ps
import logging
from src.utility.logger import logger
import uuid
from datetime import datetime
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

# data = {'timestamp': 2210714831115, 'jsonGraph': {'xname': 'Fuel Type', 'yname': 'Amount', 'type': 'scatter', 'x': ['gas', 'gas'], 'y': [4, 4]}}
data = {'timestamp': 2210714831115, 'jsonGraph': {'xname': 'Time',
                                                  'yname': 'Amount', 'type': 'scatter', 'x': 22107148311151, 'y': 5}}



values = {"jsonGraph":  {"x": data["jsonGraph"]["x"],
                         "y": data["jsonGraph"]["y"]}}
print(data["jsonGraph"].pop("x"))
print(data["jsonGraph"])
data["jsonGraph"].pop("y")


