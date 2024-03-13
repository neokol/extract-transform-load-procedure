import requests
from kafka import KafkaProducer
import json
import configuration

# Kafka Configuration
kafka_producer = KafkaProducer(
    bootstrap_servers=configuration.BOOTSTRAP_SERVER, 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to Fetch Data from Flask API
def fetch_data_from_api(start_date:int, end_date:int):
    """
    Fetches band data from the Flask API within a specified date range.

    Args:
        start_date (int): The start year for filtering bands.
        end_date (int): The end year for filtering bands.

    Returns:
        list: A list of band data if the request is successful; otherwise, None.
    """
    url = f'http://localhost:5000/get_bands?start_date={start_date}&end_date={end_date}'
    response = requests.get(url)
    print(response)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to Publish Messages to Kafka
def publish_messages(start_date:int, end_date:int):
    """
    Fetches band data from the Flask API and publishes it to the 'bands-topic' Kafka topic.

    Args:
        start_date (int): The start year for filtering bands.
        end_date (int): The end year for filtering bands.
    """
    data = fetch_data_from_api(start_date, end_date)
    print(data)
    if data:
        for record in data:
            kafka_producer.send('bands-topic', record)
        kafka_producer.flush()

if __name__ == '__main__':
    # Example date range
    publish_messages(1900, 2010)
