import sys
import ast
import json

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from src.conf import properties as p
from datetime import datetime

#from src.schema_conversion import convert_schema
from src.schema_conversion_dict import convert_schema
import src.utility.io as io
import src.conf.constants as c
from src.producer import publish
from src.utility.dict_tools import flatten_json

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


running = True


def process_data(metadata, region, data, data_base):
    """
    Reads schemas from properties
    Creates mapped data based on all schemas from given region 
    Publishes data to specific topic
    Persist data as parquet in data lake

    Args:
        metadata (dict): message metadata
        region (dict): region with schema information
        data (dict): car data
        data_base (dict) : base data with car id and time information

    """

    for key, value in region[c.SCHEMAS].items():
        # read schema and convert to struct
        with open(value[c.SCHEMA_PATH]) as f:
            schema = json.load(f)

        mapped_data = convert_schema(schema, data, data_base)

        # TODO mehrere topics für analyse und daten oder metadaten?
        metadata["type"] = "data"

        publish(value[c.SCHEMA_TOPIC], key=metadata,
                data=mapped_data)

        # persist processed data as parquet #TODO how is nested data optimally persisted in parquet
        flatten = flatten_json(mapped_data)
        pdf = pd.DataFrame(flatten, index=[0])

        if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PROCESSED], ["year", "month", "day"]):
            print(
                f"Failed to persist {c.PROCESSED} data to {region[c.PROCESSED]}!")


def extract_message(msg):
    """Extracts metadata information from message

    Args:
        metadata (dict): metadata from message

    Raises:
        Exception: Failure information while extraciting data

    Returns:
        tuple: metadata
    """
    try:
        key = msg.key().decode('UTF-8')
        value = msg.value().decode('UTF-8')

        metadata = json.loads(msg.key())

        car_id = metadata["carId"]

        region_string = metadata["region"]
        if region_string not in p.REGIONS:
            raise Exception("Region not available!")
        region = p.REGIONS[region_string]

        timestamp_millis = msg.timestamp()[1]
        dt = datetime.fromtimestamp(timestamp_millis/1000)

        return key, value, metadata, region, timestamp_millis, dt, car_id

    except Exception as e:
        print(e)
        return None


def process_msg(msg):
    """Reads message, persists events, raw data and clean data as dataframe in parquet
    schematizes and publishes data for all schemas

    Args:
        msg (kafka message): kafka message
    """

    key, value, metadata, region, timestamp_millis, dt, car_id = extract_message(
        msg)

    # time data
    day = dt.day
    month = dt.month
    year = dt.year

    # persist raw events
    event = {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": msg.timestamp(),
        "key": key,
        "value": value
    }

    io.write_data(
        f'{region[c.RAW_EVENTS]}year={year}\month={month}\day={day}\events', 'a', json.dumps(event))

    # decode and persist raw data
    io.write_data(
        f'{region[c.RAW]}year={year}\\month={month}\\day={day}\\data', 'a', json.dumps({key: value}))

    # create pandas df with time information
    pdf = pd.DataFrame([[car_id, timestamp_millis, year, month, day]],
                       columns=["carId", "timestamp", "year", "month", "day"])

    # load data and enrich with time information
    data = json.loads(value)

    # flatten values for columnar format and schema conversion
    data = flatten_json(data)

    data_base = {
        "carId": car_id,
        "timestamp": timestamp_millis,
        "year": year,
        "month": month,
        "day": day
    }

    data.update(data_base)

    # persist preprocessed data as parquet
    pdf = pd.DataFrame.from_dict(data)
    if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PREPROCESSED], ["year", "month", "day"]):
        print(
            f"Failed to persist {c.PREPROCESSED} data to {region[c.PREPROCESSED]}!")

    process_data(metadata, region, data, data_base)


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

consume_log(["car-usa", "car-eu"])
