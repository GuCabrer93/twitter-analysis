# -*-coding:Latin-1 -*

# Code below needs to be run using spark compiler, located at $SPARK_HOME/bin/spark-submit
# AND adding option --jars /path/to/
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

from re import sub
from json import loads
from emoji import get_emoji_regexp
from textblob import TextBlob

def remove_emoji(text):
    return get_emoji_regexp().sub(u'', text)

def get_sentiment(text):
    sentiment = TextBlob(text).sentiment.polarity
    neutral_threshold = 0.05
    
    if sentiment > neutral_threshold:
        return ('count', (1, 0, 0, 1)) # positive
    elif sentiment >= -neutral_threshold:
        return ('count', (0, 1, 0, 1)) # neutral
    else:
        return ('count', (0, 0, 1, 1)) # negative

def pack_and_send(time, reduced):
    if not reduced.isEmpty():
        (dummy,(pos, neutral, neg, total)) = reduced.first()

        json_data = {'positive': pos, 'neutral': neutral, 'negative': neg, 'total': total}
        #print(json_data)
        #To be implemented
        #response = requests.post(url, data=json_data) #Send to Kibana 

# Constant Definition
my_topics = ["my-tweets"]
kafka_server = {
    "metadata.broker.list": "localhost:9092"
    , "auto.offset.reset" : "smallest" # Please comment when processing real-time data
}

batch_interval = 2
window_length = 15*60
sliding_interval = 6
checkpointDirectory = "/home/user/Bureau/tmp"


# Creating Spark Streaming Context from Kafka
sc = SparkContext(master = "local[2]", appName = "My Twitter App")
ssc = StreamingContext(sparkContext = sc, batchDuration = batch_interval)
ssc.checkpoint(checkpointDirectory)
kafkaStream = KafkaUtils.createDirectStream(ssc, my_topics, kafka_server)



####################  Data Cleaning Begins  ####################
# Converting Json string into a dictionary, serialized object is a tuple with two fields
result = kafkaStream.map( lambda my_string: loads(my_string[1]) )

# Extracting text from dictionary
result = result.map( lambda my_dict: my_dict['text'] )

# Encoding into ascii (comment out this line if using other languages than English)
#result = result.map( lambda my_text: my_text.encode("ascii", errors="ignore").decode() )
result = result.map( lambda my_text: remove_emoji(my_text) )


# Removing strings starting by $, #, @ or http
result = result.map( lambda my_text: sub(pattern=r'http(\S+)(\s+)' ,repl=" " ,string=my_text) )
result = result.map( lambda my_text: sub(pattern=r'http(\S+)$'     ,repl=""  ,string=my_text) )

result = result.map( lambda my_text: sub(pattern=r'\@(\S+)(\s+)'   ,repl=" " ,string=my_text) )
result = result.map( lambda my_text: sub(pattern=r'\@(\S+)$'       ,repl=""  ,string=my_text) )

result = result.map( lambda my_text: sub(pattern=r'\#(\S+)(\s+)'   ,repl=" " ,string=my_text) )
result = result.map( lambda my_text: sub(pattern=r'\#(\S+)$'       ,repl=""  ,string=my_text) )

result = result.map( lambda my_text: sub(pattern=r'\$(\S+)(\s+)'   ,repl=" " ,string=my_text) )
result = result.map( lambda my_text: sub(pattern=r'\$(\S+)$'       ,repl=""  ,string=my_text) )


# Removing rt
result = result.map( lambda my_text: sub(pattern=r'^RT',repl="",string=my_text) )


# Removing space-like symbols
result = result.map( lambda my_text: my_text
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
result = result.map( lambda my_text: sub(pattern=r'\s+', repl=" ", string=my_text).strip() )


# Converting to lowercase
result = result.map( lambda my_text: my_text.lower() )

# Removing undesired characters (i.e. all non-alphabetic characters)
#result = result.map( lambda my_text: sub(pattern=r'[^a-z]',repl="",string=my_text) )

####################  Data Cleaning Ends  ####################

# Uncomment this line to print first 10 results
#result.map( lambda my_text: "gcg "+my_text+" gcg" ).pprint(20)

####################  Classification Begins  ####################

classified = result.map( lambda my_text : get_sentiment(my_text) )

#classified.pprint(10)


classified = classified.reduceByKeyAndWindow(
    func = lambda x,y : ( x[0]+y[0] , x[1]+y[1] , x[2]+y[2] , x[3]+y[3] )
    , invFunc = None
    , windowDuration = window_length
    , slideDuration = sliding_interval
)



classified.foreachRDD(pack_and_send)



####################  Classification Ends  ####################





# Start processing
ssc.start()
ssc.awaitTermination()
ssc.stop(stopGraceFully = True)
















'''
parsed = kafkaStream.map(lambda v: loads(v[1]))

# Analyse en continu des hashtags dans les tweets 
words = parsed.flatMap(lambda line: line.replace('"',' ').replace("'",' ').replace ("(",' ').replace(")",' ').replace("\\",' ').replace(".",' ').split()) 
#hashtags=words.filter(lambda w: w.findall(r'\B#\w*[a-zA-Z]+\w*',w)).map(lambda x: (x, 1)) 
hashtags=words.map(lambda x: (x, 1)) 

# fenêtre d'analyse des tweets toutes les 20 secondes 
stream_window_nbseconds=20 
stream_slide_nbseconds=20 
#lambda x, y: x - y, stream_window_nbseconds,stream_slide_nbseconds) 
hashtags_reduced = hashtags.reduceByKeyAndWindow(lambda x, y: x + y, stream_window_nbseconds,stream_slide_nbseconds) 
#hashtags_sorted = hashtags_reduced.transform(lambda w: w.sortBy(lambda x: x[1], ascending=False)) 

# Affichage et sauvegarde des résultats 
hashtags_reduced.pprint(5) 
hashtags_reduced.saveAsTextFiles("gcg")
print( type(hashtags_reduced) )



#hashtags_sorted.foreachRDD(lambda rdd: rdd.foreach(insertHashtags)) 
'''