# Imported required libraries
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from json import dumps
#streamlit


# Creating Constants
bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
kafka_home = "192.168.163.130:9092"
kafka_version = (0,10)



def read_from_stream():
    

    class CustomInStream(StreamingClient):
        def on_tweet(self, tweet):
            # Uncomment code below to test with kafka

            # Please note that tweepy returns an object that needs to be serialized (i.e. converted to string) 
            # It must be encoded using utf-8 to handle non-ascii characters
            producer = KafkaProducer(bootstrap_servers=kafka_home, api_version=kafka_version,  value_serializer=lambda m: dumps(m).encode('utf-8'))
            producer.send("my-tweets", value=tweet) 

            #print(tweet.id)
            #print(tweet.text)
            #print(tweet.created_at)


    # Creating streaming client and authenticating using bearer token
    streaming_client = CustomInStream(bearer_token=bearer_token)


    # Creating filtering rules
    ruleTag = "my-rule-1"

    ruleValue  = ""
    ruleValue += "lang:en"
    ruleValue += " -is:retweet"
    ruleValue += " -has:media"
    ruleValue += " Ukraine"

    rule1 = StreamRule(value=ruleValue, tag=ruleTag)

    streaming_client.add_rules(rule1, dry_run=False)

    # Adding custom fields
    tweet_fields = []
    tweet_fields.append("created_at")

    # Start reading
    streaming_client.filter(tweet_fields=tweet_fields)

    # To test without rules or custom fields
    #streaming_client.sample()
    



# Main Program starts below
read_from_stream()

