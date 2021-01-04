import os
import json
import ast
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime


type_conversion = {
    StringType(): str,
    IntegerType(): int,
    FloatType(): float,
    LongType(): int,
    BooleanType(): bool
}

#Converts value based on datatype
def convert_data_pyspark_to_python(value, data_type):
    try:
        return type_conversion.get(data_type)(value)
    except Exception as e:
        print(e)
        return value


def map_data(df_raw, struct_field):
    """  Maps raw data based on schema information to returning data array  

    Args:
        df_raw ([pandas dataframe]): source data frame with raw data
        struct_field ([pyspark StructField]): column with schema information

    Returns:
        data: mapped data array
    """
    data = []
    field = struct_field.name

    # Read correlating field if exists
    if "corr_field" in struct_field.metadata:
        field = struct_field.metadata["corr_field"]

    # Map data by field name and convert field to given datatype
    if field in df_raw.columns:
        for value in df_raw[field]:
            data.append(convert_data_pyspark_to_python(
                value, struct_field.dataType))

    return data


# Struct
def resolve_mapping(schema, df_raw):
    """Takes pyspark schema from json and recursivly maps all columns from raw data to schematised dataframe

    Args:
        schema (pyspark StructType): Pyspark schema definition
        df_raw (pandas data frame): raw data

    Returns:
        df [pyspark data frame]: schematised dataframe with mapped data
    """

    def resolve_subtyes(field, df_raw):
        """Checks for nested structures. 
        If the schema is not nested the data gets mapped and returned to the overlaying function, otherwise a recursive call for the nested object is made

        Args:
            field (pyspark struct object): Struct or field with schema information
            df_raw (pandas data frame): raw data

        Returns:
            data: pandas dataframe with mapped data (possibly nested)
        """

        if type(field.dataType) is StructType:
            data = []

            for field in field.dataType.fields:
                data.append(resolve_subtyes(field, df_raw))

            # Transpose data (dont use numpy since this infers a dtype schema)
            data_t = [[data[j][i] for j in range(len(data))] for i in range(len(data[0]))]
            return data_t
        else:
            return map_data(df_raw, field)

    # initialize array with row length from raw data and column length of schema
    data = []

    for field in schema:
        data.append(resolve_subtyes(field, df_raw))

    # Transpose data (dont use numpy since this infers a dtype schema)
    data_t = [[data[j][i] for j in range(len(data))] for i in range(len(data[0]))]

    return data_t  


def convert_schema(spark, schema, df_raw):
    """reads and maps data from df_raw with schema information
    returns spark df

    Args:
        schema (StructType): schema and mapping information
        df_raw (pandas df): raw data
    """

    df = resolve_mapping(schema, df_raw)
    return spark.createDataFrame(df, schema)