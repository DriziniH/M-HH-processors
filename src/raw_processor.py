from src.conf import properties as p
from src.schema_conversion_dict import convert_schema
import src.utility.io as io
import src.conf.constants as c
from src.producer import publish
from src.utility.dict_tools import flatten_json
from src.conf import properties_mongo as pm
from src.utility.mongo_db import MongoDB

import sys
import ast
import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime
import pandas as pd


running = True

mongo = MongoDB(pm.processed_db_con, "M-HH-car")


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

        # persist processed data to mongodb
        # TODO this should be done by another script reading the processed topic and persisting it to mongodb
        mongo.upsert_to_mongodb(mongo.get_collection("usa_car"),"carId", mapped_data["carId"], mapped_data)

        # Add schema information for processed data
        metadata["schema"] = key

        publish(value[c.SCHEMA_TOPIC], key=metadata,
                data=mapped_data)

        processed_data = {
            "metadata": metadata,
            "data": mapped_data
        }

        io.write_json_lines(
            f'{region[c.PROCESSED]}year={mapped_data["year"]}\\month={mapped_data["month"]}\\day={mapped_data["day"]}\\car.json', "a", processed_data)

        # persist processed data as parquet
        #flatten = flatten_json(mapped_data)
        # pdf = pd.DataFrame(flatten, index=[0])
        # if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PROCESSED], ["year", "month", "day"]):
        #     print(
        #         f"Failed to persist {c.PROCESSED} data to {region[c.PROCESSED]}!")


def extract_region_from_topic(topic):
    for region, dic in p.REGIONS.items():
        for _, value in dic[c.TOPICS].items():
            if topic == value:
                return dic
    return {}


def extract_message(msg):
    """Extracts metadata information from message

    Args:
        metadata (dict): metadata from message

    Raises:
        Exception: Failure information while extracting data

    Returns:
        tuple: metadata
    """
    try:
        key = msg.key().decode('UTF-8')
        value = msg.value().decode('UTF-8')

        metadata = json.loads(msg.key())

        car_id = metadata["carId"]

        region = extract_region_from_topic(msg.topic())
        if not region:
            raise Exception("Region not available!")

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

    # load data and enrich with time information
    data = json.loads(value)

    io.write_data(
        f'{region[c.RAW_EVENTS]}year={year}\month={month}\day={day}\{msg.topic()}', 'a', json.dumps(event))

    # persist raw data
    raw_data = {
        "metadata": metadata,
        "data": data
    }
    io.write_json_lines(
        f'{region[c.RAW]}year={year}\\month={month}\\day={day}\\car.json', "a", raw_data)

    # create pandas df with time information
    pdf = pd.DataFrame([[car_id, timestamp_millis, year, month, day]],
                       columns=["carId", "timestamp", "year", "month", "day"])

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
    # pdf = pd.DataFrame.from_dict(data)
    # if not io.write_partitioned_parquet_from_pandas(pdf, region[c.PREPROCESSED], ["year", "month", "day"]):
    #     print(
    #         f"Failed to persist {c.PREPROCESSED} data to {region[c.PREPROCESSED]}!")

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

def start_raw_processor():
    consume_log(["car-usa"])
