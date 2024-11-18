import boto3
import json
import yaml
import random
import sqlalchemy
from sqlalchemy import text
from time import sleep
import logging
from datetime import datetime
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load database credentials from the db_creds.yaml file
with open('db_creds.yaml', 'r') as file:
    db_creds = yaml.safe_load(file)

class AWSDBConnector:
    def __init__(self, db_creds):
        self.HOST = db_creds["host"]
        self.USER = db_creds["user"]
        self.PASSWORD = db_creds["password"]
        self.DATABASE = db_creds["database"]
        self.PORT = db_creds["port"]
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine

# Initialize the database connector
new_connector = AWSDBConnector(db_creds)

# Custom JSON encoder for datetime serialization
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 format
        return super(DateTimeEncoder, self).default(obj)

def send_data_to_kinesis(data, stream_name, partition_key):
    """
    Sends a single record to an Amazon Kinesis stream via API Gateway.

    Args:
        data (dict): The data dictionary containing column_name:value pairs.
        stream_name (str): The name of the Kinesis stream to send the data to.
        partition_key (str): The partition key for the Kinesis stream.

    Returns:
        None
    """
    # API Invoke URL
    invoke_url = f"https://4qur6lkcri.execute-api.us-east-1.amazonaws.com/test/streams/{stream_name}/record"
    
    # Prepare the payload
    payload = json.dumps({
        "StreamName": stream_name,
        "Data": data,  # Use the entire data dict as the payload
        "PartitionKey": partition_key
    }, cls=DateTimeEncoder)

    headers = {'Content-Type': 'application/json'}
    
    try:
        # Send the data via HTTP PUT request
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)
        response.raise_for_status()  # Raise exception for HTTP errors
        logger.info(f"Successfully sent data to Kinesis stream '{stream_name}': {data}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send data to Kinesis stream '{stream_name}': {e}")

def run_infinite_post_data_stream():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Fetch a row from the Pinterest table and send to the corresponding Kinesis stream
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_data_to_kinesis(pin_result, "streaming-0affd9571f39-pin", partition_key=str(pin_result["unique_id"]))

            # Fetch a row from the Geolocation table and send to the corresponding Kinesis stream
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                print(geo_result)
                send_data_to_kinesis(geo_result, "streaming-0affd9571f39-geo", partition_key=str(geo_result["ind"]))

            # Fetch a row from the User table and send to the corresponding Kinesis stream
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_data_to_kinesis(user_result, "streaming-0affd9571f39-user ", partition_key=str(user_result["ind"]))

            # Logging to monitor the sent data
            logger.info(f"Pin data sent: {pin_result}")
            logger.info(f"Geo data sent: {geo_result}")
            logger.info(f"User data sent: {user_result}")

if __name__ == "__main__":
    run_infinite_post_data_stream()
