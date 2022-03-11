from kafka import KafkaProducer

kafka_server  = ""
#kafka_server += "192.168.163.130"
kafka_server += "sandbox-hdp.hortonworks.com"
#kafka_server += "172.18.0.2" #Verified
#kafka_server += ":9092"
kafka_server += ":6667" #verified
kafka_version = (0,10,1) #verified

producer = KafkaProducer(bootstrap_servers=kafka_server, api_version=kafka_version)


if producer.bootstrap_connected():
    print("Connected")
producer.send("my-tweets", value="Test String") 

