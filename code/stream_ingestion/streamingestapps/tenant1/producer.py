import json
import logging
import os
import sys
from time import sleep

import dotenv
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS')
INPUT_FILE = 'data/data.json'

logfile = f"logs/producer_tenant1_{os.getppid()}.log"
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

if not KAFKA_BROKERS:
    raise Exception("KAFKA_BROKERS must be set")


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)

    with open(INPUT_FILE) as f:
        messages = json.load(f)

    for message in messages:
        payload = json.dumps(message).encode('utf-8')
        r = producer.send('tenant1_measurements', payload)
        logging.info(f"Sent {message['id']}")
        sleep(1)

    logging.info("All messages sent")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
