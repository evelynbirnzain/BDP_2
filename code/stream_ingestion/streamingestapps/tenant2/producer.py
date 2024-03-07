import json
import logging
import os
import sys
from time import sleep

import dotenv
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS')
INPUT_FILE = 'data/ajtu-isnz.csv'

logfile = f"logs/producer_tenant2_{os.getppid()}.log"
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

if not KAFKA_BROKERS:
    raise Exception("KAFKA_BROKERS must be set")


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)

    with open(INPUT_FILE) as f:
        for line in f[1:]:
            producer.send('tenant2_trips', line.encode('utf-8'))

    logging.info("All messages sent")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
