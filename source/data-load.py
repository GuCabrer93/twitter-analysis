# Imported required libraries
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from jsons import dumps
#streamlit


# Creating Constants
bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
kafka_server = "localhost:9092"

def read_from_stream(reset_rules):
    
    producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda m: dumps(m).encode('utf-8'))

    class CustomInStream(StreamingClient):
        def on_tweet(self, tweet):
            print(tweet.id)
            print(tweet.text)
            print(tweet.created_at)
            print(tweet.lang)

            # Please note that tweepy returns an object that needs to be serialized (i.e. converted to string) 
            # It must be encoded using utf-8 to handle non-ascii characters
            #producer.send("my-tweets", value=tweet) 

            


    # Creating streaming client and authenticating using bearer token
    streaming_client = CustomInStream(bearer_token=bearer_token)

    if (reset_rules):

        # Getting Previous Rules ID's
        prev_rules = streaming_client.get_rules()
        rules_ids = []
        for prev_rule in prev_rules[0]:
            rules_ids.append(prev_rule.id)
        
        # Deleting previous rules
        streaming_client.delete_rules( ids=rules_ids )

        # Creating filtering rules
        ruleTag = "my-rule-1"

        ruleValue  = ""
        ruleValue += "Ukraine"
        ruleValue += " lang:en"
        ruleValue += " -is:retweet"
        ruleValue += " -has:media"
        

        rule1 = StreamRule(value=ruleValue, tag=ruleTag)

        streaming_client.add_rules(rule1, dry_run=False)


    # Adding custom fields
    tweet_fields = []
    tweet_fields.append("created_at")
    tweet_fields.append("lang")

    # Start reading
    streaming_client.filter(tweet_fields=tweet_fields)

    # To test without rules or custom fields
    # streaming_client.sample()
    



# Main Program starts below

# Reading from Stream
read_from_stream( reset_rules=False )

