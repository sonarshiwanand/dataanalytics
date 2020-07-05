import json
import boto3
import os
import uuid
import base64

def lambda_handler(event, context):
    s3_client = boto3.client("s3", region_name='us-east-1')

    decoded_record_data = [base64.b64decode(record['kinesis']['data']) for record in event['Records']]
    deserialized_data = [json.loads(decoded_record) for decoded_record in decoded_record_data]

    for item in deserialized_data:
        object_key = str(uuid.uuid4())
        tweet = json.dumps(item)
        s3_client.put_object(Bucket='tweepydatastream', Key=object_key, Body=bytes(tweet, 'utf-8'))
    
    response = {'status':200}
    return response