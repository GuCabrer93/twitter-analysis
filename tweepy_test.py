# Imported required libraries
from tweepy import StreamingClient
from kafka import KafkaProducer
from json import dumps
#streamlit


# Creating Constants
bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
kafka_home = "192.168.163.130:9092"
kafka_version = (0,10)



print("Hello, World!")



def read_from_stream():
    producer = KafkaProducer(bootstrap_servers=kafka_home, api_version=kafka_version,  value_serializer=lambda m: dumps(m).encode('utf-8'))

    class CustomInStream(StreamingClient):
        def on_tweet(self, tweet):
            print(tweet.id)
            #print(tweet.text)
            producer.send("my-tweets", value=tweet)

    streaming_client = CustomInStream(bearer_token=bearer_token)
    streaming_client.sample()


def create_stream_filters():
    print("Creating filters...")

create_stream_filters()
print("Hello, World!")

