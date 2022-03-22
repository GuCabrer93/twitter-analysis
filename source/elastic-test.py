# -*-coding:Latin-1 -*
'''
print("Hello, World!")

#import org.apache.spark.SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext



sc = SparkContext(master = "local[2]", appName = "My Twitter App")

#sqlContext = SQLContext(sc)

#df = sqlContext.read.format("org.elasticsearch.spark.sql")#.load("index/type")
#df.printSchema()

#--driver-class-path=/path/to/elasticsearch-hadoop.jar


conf = {"es.resource" : "kibana_sample_data_flights"}   # assume Elasticsearch is running on localhost defaults



rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat", "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
print(rdd.first())



print("Hello, World!!")
'''

print("Hello, World")

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.rdd import RDD

sc = SparkContext(master = "local[2]", appName = "my_elastic_search")
#sc.setLogLevel("INFO")
spark = SQLContext(sc)

reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option("es.nodes.wan.only","true").option("es.port","9200").option("es.net.ssl","false").option("es.nodes", "http://localhost")

print(type(reader))

df = reader.load("schools")
print( "loaded" )

df.show()


writer = my_df.write.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option("es.nodes.wan.only","true").option("es.port","9200").option("es.net.ssl","false").option("es.nodes", "http://localhost").mode("Append")
writer.save("test1") #The name of my future index

