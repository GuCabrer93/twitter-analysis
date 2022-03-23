# -*-coding:Latin-1 -*

# Code below needs to be run using spark compiler, located at $SPARK_HOME/bin/spark-submit
# AND adding option --jars /path/to/
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark import SparkContext


# Data cleaning libraries
from json import loads

# Data classification libraries
from textblob import TextBlob


# Using textblob to get sentiment from tweets
def get_sentiment(text):
    sentiment = TextBlob(text).sentiment.polarity
    neutral_threshold = 0.05
    
    if sentiment > neutral_threshold:
        return (1, 0, 0, 1) # positive
    elif sentiment >= -neutral_threshold:
        return (0, 1, 0, 1) # neutral
    else:
        return (0, 0, 1, 1) # negative


# Format data so we can send it over to Elasticsearch
def pack_and_send(time, reduced_data):
    if not reduced_data.isEmpty():

        #(pos, neutral, neg, total) = reduced_data.first()
        
        # Adding timestamp
        reduced_data = reduced_data.map( lambda line: (line[0], line[1], line[2], line[3], time) )

        # Creating Dataframe
        df_schema = ['positive', 'neutral', 'negative', 'total', 'timestamp']
        my_df = reduced_data.toDF(df_schema)

        #my_df.printSchema()
        #my_df.show()

        writer = my_df.write.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option("es.nodes.wan.only","true").option("es.port","9200").option("es.net.ssl","false").option("es.nodes", "http://localhost").mode("Append")
        writer.save("test2") #The name of my future index
        print('Done')

        #print(json_data)
        # Yet to implement code to send data over to Kibana
        #response = requests.post(url, data=json_data) #Send to Kibana 


from emoji import get_emoji_regexp
from re import sub

# Defining function to remove emojis
def remove_emoji(text):
    return get_emoji_regexp().sub(u'', text)

def clean_data(input):
    # Removing emojis
    output = input.map( lambda my_text: remove_emoji(my_text) )

    # Encoding into ascii (comment out this line if using other languages than English)
    #output = output.map( lambda my_text: my_text.encode("ascii", errors="ignore").decode() )

    # Removing strings starting by $, #, @ or http
    output = output.map( lambda my_text: sub(pattern=r'http(\S+)(\s+)' ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'http(\S+)$'     ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\@(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\@(\S+)$'       ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\#(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\#(\S+)$'       ,repl=""  ,string=my_text) )

    output = output.map( lambda my_text: sub(pattern=r'\$(\S+)(\s+)'   ,repl=" " ,string=my_text) )
    output = output.map( lambda my_text: sub(pattern=r'\$(\S+)$'       ,repl=""  ,string=my_text) )

    # Removing retweets
    output = output.map( lambda my_text: sub(pattern=r'^RT',repl="",string=my_text) )

    # Removing space-like symbols
    output = output.map( lambda my_text: my_text
        .replace( "("  ,' ')
        .replace( ")"  ,' ')
        .replace( "["  ,' ')
        .replace( "]"  ,' ')
        .replace( "{"  ,' ')
        .replace( "}"  ,' ')
        .replace( "\\" ,' ')
        .replace( "/" ,' ')
        .replace( "#"  ," ")
        .replace( "@"  ," ")
        .replace( "$"  ," ")
        .replace( "?"  ," ")
        .replace( "!"  ," ")
        .replace( ":"  ,' ')
        .replace( ";"  ,' ')
        .replace( "."  ,' ')
        .replace( ","  ," ")
        .replace( '"'  ,' ')
        .replace( "'"  ,' ')
    )

    # Removing undesired spaces
    output = output.map( lambda my_text: sub(pattern=r'\s+', repl=" ", string=my_text).strip() )

    # Converting to lowercase
    output = output.map( lambda my_text: my_text.lower() )

    # Removing undesired characters (i.e. all non-alphabetic characters)
    #output = output.map( lambda my_text: sub(pattern=r'[^a-z]',repl="",string=my_text) )

    # Uncomment this line to print first 20 results
    #result.map( lambda my_text: "gcg "+my_text+" gcg" ).pprint(20)

    return output



########################################  BEGIN MAIN FUNCTION  ########################################

# Spark Context Configuration
master_name = "local[2]"
app_name = "My Twitter App"
checkpointDirectory = "/home/user/Bureau/tmp"

# Spark uses settings below for windowing and batch load, all values are in seconds by default
batch_interval   = 10   
window_length    = batch_interval * 1
sliding_interval = batch_interval * 1

# Kafka Configuration
my_topics = ["my-tweets"]
kafka_server = {
    "metadata.broker.list": "localhost:9092"
    , "auto.offset.reset" : "smallest" # Please comment when processing real-time data
}

# Elasticsearch Setup


# Creating Kafka Spark-Streaming Context
sc = SparkContext(master = master_name, appName = app_name)
ssc = StreamingContext(sparkContext = sc, batchDuration = batch_interval)
ssc.checkpoint(checkpointDirectory)
kafka_stream = KafkaUtils.createDirectStream(ssc, my_topics, kafka_server)
spark = SparkSession(sc)


########################################  Data Cleaning Begins  ########################################
# Converting Json string into a dictionary, serialized object is a tuple with two fields
my_dicts = kafka_stream.map( lambda my_string: loads(my_string[1]) )

# Extracting text from dictionary
my_texts = my_dicts.map( lambda my_dict: my_dict['text'] )

# Cleaning data
cleaned = clean_data(my_texts)
########################################  Data Cleaning Ends  ########################################


########################################  Classification Begins  ########################################
# Classify sentiment, one-hot encoding+total (positive, neutral, negative, total)
classified = cleaned.map( lambda my_text : get_sentiment(my_text) )

# Calculating totals by window, one-hot encoding+total (positive, neutral, negative, total)
classified = classified.reduceByWindow(
    reduceFunc = lambda x,y : ( x[0]+y[0] , x[1]+y[1] , x[2]+y[2] , x[3]+y[3] )
    , invReduceFunc = None
    , windowDuration = window_length
    , slideDuration = sliding_interval
)

# Formating and sending data to Kibana
classified.foreachRDD(pack_and_send)
########################################  Classification Ends  ########################################



########################################  Launch Data Pipeline  ########################################
ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)

########################################  END MAIN FUNCTION  ########################################

