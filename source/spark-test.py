# -*-coding:Latin-1 -*

# Code below needs to be run using spark compiler, located at $SPARK_HOME/bin/spark-submit
# AND adding option --jars /path/to/
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

from re import sub
from json import loads



# Constant Definition
my_topics = ["my-tweets"]
kafka_server = {
    "metadata.broker.list": "localhost:9092"
    #, "startingOffsets":"earliest"
    , "auto.offset.reset" : "smallest"
}
batchDuration = 5
checkpointDirectory = "/home/user/Bureau/tmp"


# Creating Spark Streaming Context from Kafka
sc = SparkContext(master = "local[2]", appName = "My Twitter App")
ssc = StreamingContext(sparkContext = sc, batchDuration = batchDuration)
ssc.checkpoint(checkpointDirectory)
kafkaStream = KafkaUtils.createDirectStream(ssc, my_topics, kafka_server)


####################  Data Cleaning Begins  ####################
# Converting Json string into a dictionary, serialized object is a tuple with two fields
result = kafkaStream.map( lambda my_string: loads(my_string[1]) )

# Extracting text from dictionary
result = result.map( lambda my_dict: my_dict['text'] )

# Encoding into ascii (comment out this line if using other languages than English)
result = result.map( lambda my_text: my_text.encode("ascii", errors="ignore").decode() )

# Converting to lowercase
result = result.map( lambda my_text: my_text.lower() )

# Removing space-like symbols
result = result.map( lambda my_text: my_text
    .replace( '"'  ,' ')
    .replace( "'"  ,' ')
    .replace( "("  ,' ')
    .replace( ")"  ,' ')
    .replace( "["  ,' ')
    .replace( "]"  ,' ')
    .replace( "{"  ,' ')
    .replace( "}"  ,' ')
    .replace( "\\" ,' ')
    .replace( "."  ,' ')
    .replace( "?"  ," ")
    .replace( ","  ," ")
)

# Removing undesired spaces
result = result.map( lambda my_text: sub(pattern=r'\s+',repl=" ",string=my_text) )

#Removing strings starting by #, @ or http
# To be implemented
#result = result.map( lambda my_text: sub(pattern=r'\s+',repl=" ",string=my_text) )

# Removing undesired characters (i.e. all non-alphabetic characters)
#result = result.map( lambda my_text: sub(pattern=r'[^a-z]',repl="",string=my_text) )

# Uncomment this line to print first 10 results
result.pprint(10)



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
print("Hello, World!")

# demarrage de la boucle de traitement des tweets 
ssc.start()
ssc.awaitTermination()



print("Hello, World!!")
