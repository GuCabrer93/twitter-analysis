#pip3 install kafka
#pip3 install kafka-python
from kafka import KafkaProducer
from json import dumps

# Creating Kafka constants
kafka_server  = ""
kafka_server += "localhost"
kafka_server += ":9092"


# Cloudera configurations below
#kafka_server += "192.168.163.130"
#kafka_server += "sandbox-hdp.hortonworks.com"
#kafka_server += "172.18.0.2" #cloudera
#kafka_server += ":6667" #cloudera
#kafka_version = (0,10,1)


producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda x:dumps(x).encode('utf-8'))


if producer.bootstrap_connected():
    print("Connected")
producer.send("my-tweets", value={1:"A"}) 

