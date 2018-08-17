# @author: Xinxin Tang
# email: xinxin.tang92@gmail.com
# -3
# publish info to redis from kafka consumer

import redis
import logging
import argparse
import atexit
from kafka import KafkaConsumer

# log config
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


if __name__ == "__main__":
    # command line config
    parser = argparse.ArgumentParser()
    parser.add_argument("topic_name", help="the topic kafka consume from")
    parser.add_argument("kafka_broker")
    parser.add_argument("redis_channel", help='get message if subscribe channel ')
    parser.add_argument("redis_host")
    parser.add_argument("redis_port")

    # reassign commands
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    redis_channel = args.redis_channel
    redis_host = args.redis_host
    redis_port = args.redis_port

    # connect to kafka consumer from target topic
    kafka_consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

    # connect to redis
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port)

    for msg in kafka_consumer:
        logger.info("Receive new data from kafka {}".format(msg.value))
        redis_client.publish(redis_channel, msg.value)
