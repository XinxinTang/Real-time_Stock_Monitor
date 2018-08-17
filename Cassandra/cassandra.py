# @author: Xinxin Tang
# email: xinxin.tang92@gmail.com
# -3
# store stock code, trade time and trade price

import argparse
import logging
import json
import atexit
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster


class data_store():

    def __init__(self, topic, kbroker, cbroker, keyspace, data_table):
        self._topic = topic
        self._kbroker = kbroker
        self._cbroker = cbroker
        self._keyspace = keyspace
        self._data_table = data_table
        self.consumer = KafkaConsumer(topic, bootstrap_servers=kbroker)
        self.cassandra_cluster = Cluster(contact_points=cbroker.split(','))
        self.cassandra_session = self.cassandra_cluster.connect()

    def logger(self):
        Format = "%(asctime)s - %(levelname)s - %(message)s"
        logging.basicConfig(format=Format)
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        return logger

    def create(self):
        statement_keyspace = \
            "CRATE KEYSPACE if not exists %s with replication = " \
            "{'class':'SimpleStrategy', 'replication_factor': 3} and durable_writes = 'true'" %self._keyspace
        self.cassandra_session.execute(statement_keyspace)
        self.cassandra_session.set_keyspace(self._keyspace)

        statement_table = \
            "CREATE TABLE[if not exists] %s (stock_symbol text, trade_time timestamp, trade_price float, " \
            "primary key ((stock_symbol), trade_time))" %self._data_table
        self.cassandra_session.execute(statement_table)

    def persist_data(self, stock_data):
        logger = self.logger()
        try:
            logger.debug("start to save data into cassandra {}".format(stock_data))
            parsed = json.loads(stock_data)[0]
            stock_symbol = parsed.get("StockSymbol")
            price = float(parsed.get("LastTradePrice"))
            tradeTime = parsed.get("LastTradeDataTime")

            statement_insert = "INSERT INTO {} (stock_symbol, trade_time, trade_price) " \
                               "VALUES ('{}', '{}', '{}')".format(self._data_table, stock_symbol, tradeTime, price)
            self.cassandra_session.execute(statement_insert)

            logger.info("Finished to save data for symbol: {}, price: {}, "
                        "tradetime: {}".format(stock_symbol, price, tradeTime))
        except Exception as e:
            logger.error("Failed to save data into Cassandra {}".format(e))

    def store(self):
        for msg in self.consumer:
            self.persist_data(msg.value)

    def shut_down(self):
        try:
            self.consumer.close()
            self.cassandra_session.shutdown()
        except KafkaError:
            self.logger().warning("Failed to close data consumer!")


if __name__ == "__main__":
    # command line
    parser = argparse.ArgumentParser()
    parser.add_argument("topic_name", help="kafka topic")
    parser.add_argument("kafka_broker", help="kafka broker")
    parser.add_argument("cassandra_broker", help="ip of Cassandra")
    parser.add_argument("keyspace", help="keyspace uses in Cassandra")
    parser.add_argument("data_table", help="the table to use")

    # command line
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    cassandra_broker = args.cassandra_broker
    keyspace = args.keyspace
    data_table = args.data_table

    # create cassandra keyspace and store data from kafka consumer
    store = data_store(topic_name, kafka_broker, cassandra_broker, keyspace, data_table)
    store.create()
    atexit.register(store.shut_down())
    store.store()






