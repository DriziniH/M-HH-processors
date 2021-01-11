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

consume_log("car-usa-info")


