import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import logging
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from urllib3.util.retry import Retry
from datetime import datetime

random.seed(100)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

# Custom JSON encoder for datetime serialization
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()  # Convert datetime to ISO 8601 format
        return super(DateTimeEncoder, self).default(obj)

def send_data_to_topic(data, topic):
    """
    Sends data to a specified topic by making a POST request to an AWS API Gateway endpoint.
    
    Args:
        data (dict): The data dictionary containing column_name:value pairs.
        topic (str): The topic name to send the data to.
    
    Returns:
        bool: True if the request was successful, False otherwise.
    """
    invoke_url = f"https://4qur6lkcri.execute-api.us-east-1.amazonaws.com/test/topics/{topic}"
    
    # Prepare payload for JSON with custom encoder for datetime handling
    payload = json.dumps({
        "records": [
            {
                "value": data  # Use the entire data dict as the value payload
            }
        ]
    }, cls=DateTimeEncoder)

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    
    # Retry strategy for handling intermittent issues
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"]  # Updated to use allowed_methods
    )
    
    # Session with retry configuration
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    
    try:
        response = session.post(invoke_url, headers=headers, data=payload)
        response.raise_for_status()  # Raise exception for HTTP errors
        logger.info(f"Successfully sent data to topic '{topic}': {data}")
        return True

    except RequestException as e:
        logger.error(f"Failed to send data to topic '{topic}': {e}")
        return False

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            # Fetch a row from the pin table and send to 0affd9571f39.pin topic
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                send_data_to_topic(pin_result, "0affd9571f39.pin")

            # Fetch a row from the geolocation table and send to 0affd9571f39.geo topic
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                send_data_to_topic(geo_result, "0affd9571f39.geo")

            # Fetch a row from the user table and send to 0affd9571f39.user topic
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                send_data_to_topic(user_result, "0affd9571f39.user")
            
            # Logging to monitor the sent data
            logger.info(f"Pin data sent: {pin_result}")
            logger.info(f"Geo data sent: {geo_result}")
            logger.info(f"User data sent: {user_result}")

if __name__ == "__main__":
    run_infinite_post_data_loop()
