import sys
import ast
import json

from src.conf import properties as p

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from datetime import datetime

running = True

def shutdown():
    running = False

# def deserialize_msg(msg, car_id):
#     """Reads message, persists events, raw data and clean data as dataframe in parquet
#     schematizes and publishes data for all schemas

#     Args:
#         msg (kafka message): kafka message
#     """

#     metadata = json_to_dict(msg.key().decode('UTF-8'))

#     print(metadata["carId"])
#     print(f'Consumer ID: {car_id}')

#     if "carId" in metadata and metadata["carId"] == car_id:
#         data = json_to_dict(msg.value().decode('UTF-8'))
#         print(msg.value().decode('UTF-8'))
#         print(data)
#         return metadata, data
    
#     return None, None

def print_message(msg, data):
    print("Topic: %s || Partition:%d || Offset:%d" % (msg.topic(), msg.partition(),
                                msg.offset()))
    print(f'Key: {msg.key()}')
    print(f'Value: {msg.value()}')
    print(f'Data: {data}' )
    

# def extract_metadata(metadata):
#     """Extracts metadata information from message

#     Args:
#         metadata (dict): metadata from message

#     Raises:
#         Exception: Failure information while extraciting data

#     Returns:
#         tuple: metadata
#     """
#     try:
#         region_string = metadata["region"]
#         timestamp_millis = metadata["timestamp"]
#         car_id = metadata["carID"]

#         if region_string not in p.REGIONS:
#             raise Exception("Region not available!")

#         region = p.REGIONS[region_string]
#         dt = datetime.fromtimestamp(timestamp_millis/1000.0)

#         return region, timestamp_millis, dt, car_id

#     except Exception as e:
#         print(e)
#         return None

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
        consumer.subscribe([topics])

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
                print(msg.key())
                print(msg.value())
                print(msg.timestamp()[1])

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


