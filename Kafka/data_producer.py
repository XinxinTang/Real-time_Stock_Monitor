# @author: Xinxin Tang
# email: xinxin.tang92@gmail.com
#-1
# execute flask in background
# fetch stock info using google finance
# send info to Kafka producer

import atexit
import logging
import json
import time
from googlefinance import getQuotes
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# log config
Format = "%(asctime) - 15s %(message)s"
logging.basicConfig(format=Format)
logger = logging.getLogger('data_producer')
logger.setLevel(logging.INFO)

# flask config
app = Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kakfa_broker = app.config['CONFIG_KAKFA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']

# kafka producer connects to flask
producer = KafkaProducer(bootstrap_servers=kakfa_broker)

# scheduler config
schedule = BackgroundScheduler()
schedule.add_executor('threadpool')
schedule.start()

# set of stock code without duplication
symbols = set()


def shutdown_hook():
    """
    flush data in kafka before shutdown
    """
    try:
        logger.info("Flushing pending messages to kafka, timeout is set to 10s")
        producer.flush(10)
        logger.info("Finish flushing pending message to Kafka")
    except KafkaError as KE:
        logger.warning("Failed to flush pending message to Kafka, caused by {}".format(KE))
    finally:
        try:
            logger.info("Closing Kafka connection")
            producer.close(10)
        except Exception as e:
            logger.warning("Failed to close Kafka connection, caused by {}".format(e))

    try:
        logger.info("Shutdown scheduler")
        schedule.shutdown()
    except Exception as e:
        logger.warning("Failed to shutdown scheduler, caused by {}".format(e))


def fetch_price(symbol):
    logger.debug("Start to fetch stock price for {}".format(symbol))
    try:
        price = json.dumps(getQuotes(symbol))
        logger.debug("Retrieved stock info {}".format(price))
        producer.send(topic=topic_name, value=price, timestamp_ms=time.time())
        logger.info("Sent stock price for {} to Kafka".format(symbol))
    except KafkaTimeoutError as TE:
        logger.warning("Failed to send stock price for {} to Kafka, caused by : {}".format(symbol, TE))
    except Exception as e:
        logger.warning("Failed to fetch stock price for {}".format(e))


@app.route('/<symbol>/add', methods=['POST'])
def add_stock(symbol):
    if not symbol:
        return jsonify({'error': 'Stock symbol cannot be empty'}), 400
    if symbol in symbols:
        pass
    else:
        symbol = symbol.encode('utf-8')
        symbols.add(symbol)
        logger.info("Add stock retrieve job {}".format(symbol))
        schedule.add_job(fetch_price, 'interval', [symbol], seconds=1, id=symbol)
    return jsonify(results=list(symbols)), 200


@app.route('/<symbol>/delete', methods=['POST'])
def del_stock(symbol):
    logger.info("Remove the {}".format(symbol))
    if not symbol:
        return jsonify({'Error': 'Stock symbol cannot be empty'}), 400
    if symbol not in symbols:
        pass
    else:
        symbol = symbol.encode('utf-8')
        logger.info("Remove the {}".format(symbol))
        symbols.remove(symbol)
        schedule.remove_job(symbol)
    return jsonify(results=list(symbols)), 200


if __name__ == "__main__":
    atexit.register(shutdown_hook)
    app.run(host='0.0.0.0', port=app.config['CONGIF_APPLICATION_PORT'])



