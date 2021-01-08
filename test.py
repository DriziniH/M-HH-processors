import json
from src.schema_conversion_dict import convert_schema
import src.io as io
import random
import ast
import uuid
from src.conf import properties_showcase as ps
from src.extract_schema_from_json import extract_schema
from src.flatten_json_data import flatten_json
import pandas as pd
import numpy

data = ps.car_data_eu

# print(flatten_json(data))

# print(json.dumps(extract_schema("raw_data_usa", data), sort_keys=True, indent=4))

with open("src/conf/schemas/dict_eu.json") as f:
    schema = json.load(f)
 

data = flatten_json(data)

data["carId"] = "car_id"
data["timestamp"] = "timestamp_millis"
data["year"] = "year"
data["month"] = "month"
data["day"] = "day"

print(data)

df = pd.DataFrame.from_dict(data)

print(df)

json = pd.DataFrame.to_dict(df)

print(json)
