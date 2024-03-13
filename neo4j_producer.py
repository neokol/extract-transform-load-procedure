import requests
from kafka import KafkaProducer
import json
import configuration

# Kafka Producer Configuration
kafka_producer = KafkaProducer(
    bootstrap_servers=configuration.BOOTSTRAP_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to Fetch Data from Flask API
def fetch_data_from_api(user_name: str):
    """
    Fetches user data from the Flask API based on the provided user name.

    Args:
        user_name (str): The name of the user for which to fetch data.

    Returns:
        list: A list of user data if the request is successful; otherwise, None.
    """
    url = f'http://localhost:5000/get_user_data?name={user_name}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to Publish Messages to Kafka
def publish_messages(user_name: str):
    """
    Fetches user data from the Flask API and publishes it to the 'users-topic' Kafka topic.

    Args:
        user_name (str): The name of the user for which to fetch and publish data.
    """
    data = fetch_data_from_api(user_name)
    print(data)
    if data:
        for record in data:
            kafka_producer.send('users-topic', record)
        kafka_producer.flush()

if __name__ == '__main__':
    # Example user name
    publish_messages('Neo')
