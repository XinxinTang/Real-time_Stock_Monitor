# Stock_Monitor_System
Cassandra, Redis, Kafka, Spark-streaming, Elastic-search, Kibana, Flask

## Introduction
In this project, We use Kafka as the high volume data transmitter, Cassandra as the NoSQL database, Spark can do streaming process, ElasticSearch as the fast search engine, node.js as the server.

## Data process
We fetched stock info using google finance API and transmitted the info to Kafka producer; Then called spark-streaming processed the raw data from KafkaBroker and got the average price of stock of every timestamp; Pushed the data to redis hub for server to read; Finally, displaying the real-time dynamic data using Bootstrap，jQuery and D3.js. 


## Structure
![System structure](https://github.com/XinxinTang/Stock_Monitor_System/blob/master/Images/Screen%20Shot%202018-02-05%20at%202.22.57%20PM.png)

## Get started

1. Suppose the IP is 192.168.99.100, run data_producer.py with port, kafka broker ip, kafka topic in dev.cfg file </br>
`export ENV_CONFIG_FILE=`pwd`/config/dev.cfg` </br>
then run </br>
`python data_producer.py`

2. Run Redis </br>
`python redis.py 'kakfa_topic' 192.168.99.100:9092 'redis_channel' 192.168.99.100:6379`

3. Run spark streaming </br>
`spark-submit spark_streaming.py 'kafka_producer_topic' 'target_kafka_topic' 192.168.99.100:9092`

4. Then start server </br>
`node index.js --port=3000 --redis_host=192.168.99.100 --redis_port=6379 --subscribe_topic='kafka target topic'`

5. For backup, start running Cassandra </br>

6. For searching and visualization, start running ElasticSearch then monitor the info from Kibana </br>
