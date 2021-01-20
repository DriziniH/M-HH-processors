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

string = "{'id': 'c2e1198f-bb52-4e8b-be0b-9685e85bc81a'}"
value = "{'model': 'G-Klasse', 'labels': ['Benzer', 'E', 'Immer Vorfahrt'], 'fuel': 'gas', 'kilometer_total': 51858.94028299327, 'kilometer': 473.3565746618418, 'travel_time_total': 6073306280, 'travel_time': 98, 'oil_level': 2.6180029800087157, 'break_fluid_level': 1.8136505379063417, 'fuel_level': 4.449230608477634, 'engine': 41.481012236512704, 'breaks': 44.848899187490865, 'tire_pressure': 50.43044519928234, 'temperature': {'engine': 1.5136384782733825, 'car': 61.801204189441506, 'breaks': 4.931898082996233, 'tires': 47.38930900548435}, 'break_power': 22.69590299590155, 'break': False, 'gas_power': 86.40649740907999, 'gas': False, 'light': False, 'acc': 33.647135286809814, 'kmh': 57.49083299832871, 'rpm': 5332.532560444217, 'oxygen_level': 83.89552791258146, 'pos': {'lat': 50.1812028409468, 'lon': 18.92672185951993}, 'infotainment': {'on': False, 'service': 'Mobile Phone', 'volume': 89.42445299009376}}"

print(type(dict_tools.load_json_to_dict(value)))
