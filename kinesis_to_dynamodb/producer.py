import boto3
import json
import time
import random

STREAM_NAME = "iot_data_stream"

kinesis = boto3.client('kinesis', endpoint_url='http://localhost:4566', region_name='us-east-1')

def generate_iot_data():
    return {
        "device_id": f"device_{random.randint(1, 10)}",
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 60.0), 2),
        "timestamp": int(time.time())
    }

while True:
    data = generate_iot_data()
    kinesis.put_record(StreamName=STREAM_NAME, Data=json.dumps(data), PartitionKey=data["device_id"])
    print(f"Sent data: {data}")
    time.sleep(1)
