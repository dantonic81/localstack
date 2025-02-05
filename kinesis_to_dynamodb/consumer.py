import boto3
import json
import time
from decimal import Decimal

STREAM_NAME = "iot_data_stream"
TABLE_NAME = "iot_data_table"

kinesis = boto3.client('kinesis', endpoint_url='http://localhost:4566', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:4566', region_name='us-east-1')
table = dynamodb.Table(TABLE_NAME)

def process_kinesis_records():
    response = kinesis.get_shard_iterator(StreamName=STREAM_NAME, ShardId='shardId-000000000000', ShardIteratorType='TRIM_HORIZON')
    shard_iterator = response['ShardIterator']

    while True:
        records_response = kinesis.get_records(ShardIterator=shard_iterator)
        records = records_response['Records']

        for record in records:
            data = json.loads(record['Data'])
            print(f"Processing record: {data}")

            # Convert float values to Decimal
            data['temperature'] = Decimal(str(data['temperature']))
            data['humidity'] = Decimal(str(data['humidity']))
            data['timestamp'] = Decimal(str(data['timestamp']))

            # Store in DynamoDB
            table.put_item(Item=data)

        shard_iterator = records_response['NextShardIterator']
        time.sleep(2)

if __name__ == "__main__":
    process_kinesis_records()
