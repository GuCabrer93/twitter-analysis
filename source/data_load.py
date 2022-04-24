# Imported required libraries
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from jsons import dumps
#streamlit

# Creating reading class 
class CustomInStream(StreamingClient):
        
        def __init__(self, bearer_token, kafka_topic):
            super(CustomInStream, self).__init__(bearer_token=bearer_token)
            self.producer = KafkaProducer(bootstrap_servers=kafka_server, value_serializer=lambda m: dumps(m).encode('utf-8'))
            self.kafka_topic = kafka_topic

        def on_tweet(self, tweet):
            # Please note that tweepy returns an object that needs to be serialized (i.e. converted to string) 
            # It must be encoded using utf-8 to handle non-ascii characters
            print(tweet.id)
            self.producer.send(self.kafka_topic, value=tweet) 
            

            #print(tweet.text)
            #print(tweet.created_at)
            #print(tweet.lang)

            


def recompile_rules(streaming_client):
    # Getting Previous Rules ID's
    prev_rules = streaming_client.get_rules()
    rules_ids = []
    for prev_rule in prev_rules[0]:
        rules_ids.append(prev_rule.id)
    
    # Deleting previous rules
    streaming_client.delete_rules( ids=rules_ids )

    # Creating filtering rules
    ruleTag = "my-rule-1"
    ruleValue = ""

    ruleValue += "("
    ruleValue += "covid"
    ruleValue += " -is:retweet"
    ruleValue += " -is:reply"
    ruleValue += " -is:quote"
    ruleValue += " -has:media"
    ruleValue += " lang:en"
    ruleValue += ")"

    ruleValue += " OR ("
    ruleValue += "covid"
    ruleValue += " -is:retweet"
    ruleValue += " -is:reply"
    ruleValue += " -is:quote"
    ruleValue += " -has:media"
    ruleValue += " lang:fr"
    ruleValue += ")"

    
    # Adding new rules
    rule1 = StreamRule(value=ruleValue, tag=ruleTag)
    streaming_client.add_rules(rule1, dry_run=False)

    
########################################  BEGIN MAIN FUNCTION  ########################################

# Tweepy Setup
bearer_token = "AAAAAAAAAAAAAAAAAAAAAORIaAEAAAAAl6GnXS7YGOeQdYa0uGwc8DMF40Q%3DLIM7DC8lSNbFmsVsgWYyJqYrl2iBCSsR3Z1uUqjJR8c2kONAG4"
reset_filtering_rules = False


# Kafka Configuration
kafka_server  = ""
kafka_server += "localhost"
kafka_server += ":9092"
kafka_topic   = "mes-tweets"


# Creating streaming client and authenticating using bearer token
streaming_client = CustomInStream(bearer_token=bearer_token, kafka_topic=kafka_topic)

# Recompiling client rules, if needed
if (reset_filtering_rules):
    recompile_rules(streaming_client) 

# Adding custom fields
tweet_fields = []
tweet_fields.append("created_at")
tweet_fields.append("lang")

expansions = []
expansions.append("referenced_tweets.id")
expansions.append("author_id")

# To test without rules or custom fields
# streaming_client.sample()

# Start reading
streaming_client.filter(tweet_fields=tweet_fields, expansions=expansions)

########################################  END MAIN FUNCTION  ########################################

