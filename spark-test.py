# -*-coding:Latin-1 -*

# Code below needs to be run using spark compiler, located at $SPARK_HOME/bin/spark-submit
# AND adding option --jars /path/to/
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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


sc = SparkContext(master = "local[2]", appName = "My Twitter App")
ssc = StreamingContext(sparkContext = sc, batchDuration = batchDuration)
ssc.checkpoint(checkpointDirectory)

kafkaStream = KafkaUtils.createDirectStream(ssc, my_topics, kafka_server)


kafkaStream.map(lambda tweet: loads(tweet[1])['text'] ).pprint(5)






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
