import json
from src.schema_conversion_dict import convert_schema
import src.utility.io as io
import random
import ast
import uuid
from src.conf import properties_showcase as ps
import pandas as pd
import numpy
from src.consumer import consume_log
from pymongo import MongoClient


mongo_connection = 'mongodb+srv://Hendrik:m-hh-mongo@eu-m-hh.fxa4u.mongodb.net/M-HH-analysis'
mongo_client = MongoClient(mongo_connection)

mongo_db = mongo_client["M-HH-analysis"]


regional_col = mongo_db["usa_analysis"]


curr_analysis = regional_col.find({})
    
for document in curr_analysis:
    print(document)



