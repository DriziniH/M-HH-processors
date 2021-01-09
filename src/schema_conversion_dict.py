import os
import json
import ast
import numpy as np
import pandas as pd
from datetime import datetime

type_conversion = {
    "str": str,
    "int": int,
    "float": float,
    "long": int,
    "bool": bool
}

# Converts value based on datatype
def convert_type(value, data_type):
    try:
        return type_conversion.get(data_type)(value)
    except Exception as e:
        print(e)
        return value


def map_array(df_raw, fields):
    data = []

    for field in fields:
        column = field["name"]

        # Read correlating field if exists
        if "corr_field" in field["metadata"]:
            column = field["metadata"]["corr_field"]

        # Map data by field name and convert field to given datatype
        if column in df_raw.columns:
            for value in df_raw[column]:
                data[column] = value

    return data


def map_field(data_raw, field):
    """  Maps raw data based on schema information to returning data array  

    Args:
        data_raw (dict): raw data
        field (dict): field with schema information

    Returns:
        data: mapped data dict
    """
    data = {}
    field_name = field["name"]

    # Read correlating field if exists
    if "corr_field" in field["metadata"]:
        field_name = field["metadata"]["corr_field"]

    # Map data by field name and convert field to given datatype
    if field_name in data_raw:
        return {field["name"]: data_raw[field_name]}
    else:
        return {}

    return data


def convert_schema(schema, data_raw, data = {}):
    """Takes fields from json schema and recursivly maps raw data to schematised dict

    Args:
        schema (dict): schema and mapping information
        data_raw (dict): raw data
        data (dict) : data (potentially not empty) where data is mapped to

    Returns:
        data [dict]: dict with schematized data
    """

    def resolve_subtypes(fields, df_raw):
        data = {}
        for field in fields:
            data.update(resolve_subtype(field, df_raw))

        return data

    def resolve_subtype(field, data_raw, data=None):
        """Checks for nested structures. 
        If the schema is not nested the data gets mapped and returned to the overlaying function, otherwise a recursive call resolves the nested objects

        Args:
            field (pyspark struct object): Struct or field with schema information
            data_raw (dict): raw data
            data (dict): data from previous recursion

        Returns:
            data [dict]: dict with schematized data
        """
        
        if field["type"] == "dict" or field["type"] == "list":

            # consider recursive calls
            if(data is None):
                data = {}

            subdata = resolve_subtypes(field["fields"], data_raw)

            if field["type"] == "dict":
                data[field["name"]] = subdata

            elif field["type"] == "list":
                array = []
                for _, value in subdata.items():
                    array.append(value)
                data[field["name"]] = array
            return data

        else:
            return map_field(data_raw, field)

    # initialize array with row length from raw data and column length of schema

    for field in schema["fields"]:
        data.update(resolve_subtype(field, data_raw, data))

    return data


