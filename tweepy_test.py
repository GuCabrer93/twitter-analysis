print("Hello, World!")
'''
from tweepy import StreamingClient

bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
streaming_client = StreamingClient(bearer_token=bearer_token)

streaming_client.
'''

from kafka import KafkaProducer
from json import dumps
producer = KafkaProducer(bootstrap_servers='192.168.163.130:9092', api_version=(0,10),  value_serializer=lambda m: dumps(m).encode('utf-8'))

#streamlit

