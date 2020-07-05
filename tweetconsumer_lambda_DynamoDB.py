import base64
import json
import boto3
import decimal


def lambda_handler(event, context):
    item = None
    dynamo_db = boto3.resource('dynamodb')
    table = dynamo_db.Table('tweets')
    
    decoded_record_data = [base64.b64decode(
        record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record)
                         for decoded_record in decoded_record_data]

    with table.batch_writer() as batch_writer:
        for item in deserialized_data:
            id = item['id_str']
            text = item['text']
            source = item['source']

            batch_writer.put_item(
                Item={
                    'id': id,
                    'text': text,
                    'source':source
                }
            )
