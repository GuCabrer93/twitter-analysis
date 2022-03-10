# Loading libraries
from tweepy.streaming import Stream
from tweepy import OAuth2BearerHandler, API, Client
from kafka import KafkaProducer
from json import dumps




    #producer = KafkaProducer( value_serializer=lambda m: dumps(m).encode('utf-8'), bootstrap_servers=['192.168.163.130:6667'])
    #producer = KafkaProducer( value_serializer=lambda m: dumps(m).encode('utf-8'), bootstrap_servers=['http://192.168.163.130:6667'])

#We need to find a way to create a topic in Kafka Horton works
#producer = KafkaProducer(bootstrap_servers='192.168.163.130:9092')
print("Hello, World!")


















'''
auth = OAuth2BearerHandler(bearer_token)

api = API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)
'''
bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"

streaming_client = Client(bearer_token=bearer_token)
response = streaming_client.search_recent_tweets(query="lang:en Bratislava", max_results=10)

print(response.meta)

tweets = response.data

# Each Tweet object has default id and text fields
for tweet in tweets:
    print(tweet.id)
    print(tweet.text)

print("Hello, World!")


