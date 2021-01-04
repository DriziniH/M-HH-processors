import sys
import ast
import json

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from src.conf import properties as p
from datetime import datetime

from src.schema_conversion import convert_schema
import src.io as io
import src.conf.constants as c
from src.producer import publish

from pyspark.sql.types import *
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import socket

from pyspark.sql import SparkSession


running = True


def extract_metadata(metadata):
    """Extracts metadata information from message

    Args:
        metadata (dict): metadata from message

    Raises:
        Exception: Failure information while extraciting data

    Returns:
        tuple: metadata
    """
    try:
        region_string = metadata["region"]
        timestamp_millis = metadata["timestamp"]
        car_id = metadata["carID"]

        if region_string not in p.REGIONS:
            raise Exception("Region not available!")

        region = p.REGIONS[region_string]
        dt = datetime.fromtimestamp(timestamp_millis/1000.0)

        return region, timestamp_millis, dt, car_id

    except Exception as e:
        print(e)
        return None



def schematize_and_publish(metadata, region, pdf):
    """
    Reads schemas from properties
    Creates mapped data based on all schemas from given region 
    Publishes to topic from schema

    Args:
        metadata (dict): message metadata
        region (dict): region with schema information
        pdf (pandas data frame): raw data
    """

    for key, value in region[c.SCHEMAS].items():
        # read schema and convert to struct
        with open(value[c.SCHEMA_PATH]) as f:
            json_schema = json.load(f)
        schema = StructType.fromJson(json_schema)

        mapped_data = convert_schema(schema, pdf)

        #spark.createDataFrame(mapped_data, schema)

        publish(value[c.SCHEMA_TOPIC], key=metadata,
            data=mapped_data)


def process_msg(msg):
    """Reads message, persists events, raw data and clean data as dataframe in parquet
    schematizes and publishes data for all schemas

    Args:
        msg (kafka message): kafka message
    """
    key = msg.key().decode('UTF-8')
    data = msg.value().decode('UTF-8')

    metadata = json.loads(key)

    region, timestamp_millis, dt, car_id = extract_metadata(metadata)

    # time data
    day = dt.day
    month = dt.month
    year = dt.year

    # persist raw events
    event = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "key": key,
        "value": data
    }

    io.write_data(
        f'{region[c.RAW_EVENTS]}year={year}\month={month}\day={day}\events', 'a', json.dumps(event))

    # decode and persist raw data
    io.write_data(
        f'{region[c.RAW]}year={year}\\month={month}\\day={day}\\data', 'a', json.dumps({key: data}))  # TODO Name der Datei

    # create pandas df with time information
    pdf = pd.DataFrame([[car_id, timestamp_millis, year, month, day]],
                       columns=["carId","timestamp", "year", "month", "day"])

    # split and add data to pandas df
    for entry in data.split(","):
        pdf[entry.split(":")[0].strip()] = entry.split(":")[1].strip()

    if io.write_partitioned_parquet_from_pandas(pdf, region[c.PROCESSED], ["year", "month", "day"]):
        print("Data successfully persisted!")
    else:
        print("Failed to persist data!")

    schematize_and_publish(metadata, region, pdf)


def shutdown():
    running = False


def consume_log(topics):
    """Infinitly reads kafka log from latest point

    Args:
        topics (String[]): Topics to read from

    Raises:
        KafkaException: Kafka exception
    """
    # https://docs.confluent.io/clients-confluent-kafka-python/current/index.html
    conf = {'bootstrap.servers': "localhost:9092",
            'group.id': "car",
            'auto.offset.reset': 'smallest'}

    consumer = Consumer(conf)

    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                process_msg(msg)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()



# spark = SparkSession.builder.appName("Spark Session").getOrCreate()
# # Enable Arrow-based columnar data transfers
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

consume_log(["car-usa","car-eu"])
