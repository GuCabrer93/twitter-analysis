print("Hello, World!")


#Part below works
'''
from kafka import KafkaProducer
from json import dumps
producer = KafkaProducer(bootstrap_servers='192.168.163.130:9092', api_version=(0,10),  value_serializer=lambda m: dumps(m).encode('utf-8'))
'''

#streamlit


from tweepy import StreamingClient

class CustomInStream(StreamingClient):

    def on_tweet(self, tweet):
        print(tweet.id)
        print(tweet.text)


bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
streaming_client = CustomInStream(bearer_token=bearer_token)

streaming_client.sample()

print("Hello, World!")

