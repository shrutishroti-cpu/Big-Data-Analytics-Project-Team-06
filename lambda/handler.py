import boto3
import json
import time
import random
import uuid
from datetime import datetime

# AWS clients
kinesis = boto3.client('kinesis')

# Constants
STREAM_NAME = 'data-clicks-06'

# Sample values for random generation
pages = ['home', 'product', 'cart', 'checkout', 'search']

def generate_click_event():
    """Generate a random click event."""
    return {
        'event_id': str(uuid.uuid4()),
        'timestamp': datetime.utcnow().isoformat(),
        'user_id': random.randint(1000, 9999),
        'page': random.choice(pages),
        'action': random.choice(['click', 'view', 'add_to_cart', 'purchase'])
    }

def lambda_handler(event, context):
    num_records = event.get('num_records', 50)  # Default to 10 events
    for i in range(num_records):
        record = generate_click_event()
        try:
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(record),
                PartitionKey=str(record['user_id'])
            )
            print(f"Sent record {i+1}: {record}")
        except Exception as e:
            print(f"Error sending record {i+1}: {e}")
        # Random delay between events
        time.sleep(random.uniform(0.5, 2.5))
    return {
        'statusCode': 200,
        'body': f'Successfully streamed {num_records} random click events to Kinesis.'
    }