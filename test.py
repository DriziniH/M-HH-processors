import json
from src.utility import io
# from src.conf import properties_showcase as ps
# import logging
# from src.utility.logger import logger
import uuid
# from datetime import datetime
# from src.conf import properties_mongo as pm
# from src.utility.mongo_db import MongoDB
# from src.utility import dict_tools
from src.utility import format_conversion
import numpy as np
import pandas as pd

# mongo_db = MongoDB(pm.db_con, "M-HH")

# conf_col = mongo_db.get_collection("config")

# conf = conf_col.find_one({}, {'_id': False})

# print(uuid.uuid4())


data_json = io.read_json_lines(r'C:\Showcase\Projekt\M-HH-processors\data-lake\S3-eu_processed\json\year=2021\month=1\day=21\car-eu-example.json', True)
for row in data_json:
    row.update({
        "year": 2021,
        "month": 1,
        "day": 21,
    })
pdf = pd.DataFrame(data_json)   
print(pdf) 
io.write_partitioned_parquet_from_pandas(pdf, 'C:\\Showcase\\Projekt\\M-HH-processors\\data-lake\\S3-eu_processed\\parquet\\', ["year", "month", "day"])  
