# @author: Xinxin Tang
# email: xinxin.tang92@gmail.com
# -2
# fetch data from kafka producer
# doing computation using spark streaming
# store back to kafka producer in another topic

import argparse
import json
import logging
import atexit
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka.errors import KafkaError


class spark_streaming():

    def __init__(self, topic, target_topic, kafka_broker):
        self.topic = topic
        self.kafka_broker = kafka_broker
        self.target_topic = target_topic

        self.kafka_producer = KafkaProducer(bootrap_servers=kafka_broker)
        sc = SparkContext("local[2]", "AveragePrice")
        sc.setLogLevel("INFO")
        self.ssc = StreamingContext(sc, 5)

        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

    def process(self, timeobj, rdd):
        def group(record):
            data = json.loads(record[1].decode('utf-8'))[0]
            return data.get("StockSymbol"), (float(data.get("LastTradePrice")), 1)

        newRDD = rdd.map(group).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
            .map(lambda symbol, price : (symbol, price[0]/price[1]))
        results = newRDD.collect()

        for res in results:
            msg = {"StockSymbol": res[0],
                   "AveragePrice": res[1]}
            try:
                self.kafka_producer.send(self.target_topic, value=json.dumps(msg))
                self.logger.info("Successfully send processed data to {}, {}".format(self.target_topic, msg))
            except KafkaError as KE:
                self.logger.warning("Failed to send data, the error is {}".format(msg))

    def createStream(self):
        # create space for data computation
        directKafkaStream = KafkaUtils.createDirectStream(self.ssc, [self.topic],
                                                          {"metadata.broker.list" : self.kafka_broker})
        return directKafkaStream

    def run(self):
        direceKafkaStream = self.createStream()
        direceKafkaStream.foreachRDD(self.process) # transformation with action
        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", help="this is the topic to receive data from kafka producer")
    parser.add_argument("target_topic", help="this is the topic to send processed data to kafka broker")
    parser.add_argument("kafka_broker", help="this is the kafka broker")

    args = parser.parse_args()
    topic = args.topic
    target_topic = args.target_topic
    kafka_broker = args.kafka_broker

    KafkaSpark = spark_streaming(topic, target_topic, kafka_broker)
    KafkaSpark.run()

