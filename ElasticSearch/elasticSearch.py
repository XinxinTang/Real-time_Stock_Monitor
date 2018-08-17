# @author: Xinxin Tang
# email: xinxin.tang92@gmail.com
# -3
# deliver to elastic search
# search and view data using Kibana Dashboard

import json
import logging
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class ElasticSearch():

    def __init__(self, topic, kbroker):
        self._topic = topic
        self._kbroker = kbroker

        # connect
        self.consumer = KafkaConsumer(topic, bootstrap_server=self._kbroker)
        self.es = Elasticsearch(['localhost'], http_auth=('elastic', 'changeme'), port=9200)
        self.actions = []

    def create_index(self):
        request_body = {
            "settings":{
                "number_of_shards": 5,
                "number_of_replicas": 1
            },

            'mapping': {
                'examplecase': {
                    'properties':{
                        'price': {'index': 'analyzed', 'type': 'float'},
                        'tradetime' : {'index': 'analyzed', 'format': 'dateOptionalTime', 'type': 'date'},
                    }
                }
            }
        }

        self.es.indices.create(index="test_index", body=request_body)

    def deliver(self):
        bulk = []
        for msg in self.consumer:
            parsed = json.loads(msg.value)[0]
            price = float(parsed.get("LastTradePrice"))
            tradetime = parsed.get("LastTradeDateTime")
            dict = {"price": price, "tradetime": tradetime}
            op_dict = {
                "index": {
                    "_index": 'test_index',
                    "_type": 'exapmlecase'
                }
            }
            bulk.append(op_dict)
            bulk.append(dict)
            self.es.bulk(index="test_index", body=bulk)
            bulk = []


if __name__ == "__main__":
    ES = ElasticSearch("test", "192.168.99.100:9092")
    ES.create_index()
    ES.deliver()