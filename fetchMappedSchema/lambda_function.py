import json
import boto3
from botocore.exceptions import ClientError

import schema_conversion


def lambda_handler(event, context):

    dynamodb = boto3.resource('dynamodb')
    processed_table = dynamodb.Table('M-HH')
    schema_table = dynamodb.Table('Schemas')
    
    try:
        schema_id = event["arguments"]["schema"]
        print(f'Schema ID: {schema_id}')
        schema = schema_table.get_item(Key={'id': schema_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
        return {
            'statusCode': 400,
            'body': json.dumps('Error fetching schema!')
        }
    try:
        car_id = event["arguments"]["id"]
        unit_id = event["arguments"]["unit"]
        data_source = processed_table.get_item(Key={'id': car_id, '_unit': unit_id})
    except ClientError as e:
        print(e.response['Error']['Message'])
        return {
            'statusCode': 400,
            'body': json.dumps('Error fetching data!')
        }

 
    response = schema_conversion.convert_schema(schema, data_source) if schema else data_source

    
    return {
        'statusCode': 200,
        'body': response
    }
    
