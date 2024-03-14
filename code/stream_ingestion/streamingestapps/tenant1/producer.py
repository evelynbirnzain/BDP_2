import json
import logging
import os
import sys
from time import sleep

import dotenv
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
INPUT_FILE = 'data/sensor-data.json'

logfile = f"logs/producer_tenant1_{os.getppid()}.log"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

if not KAFKA_BROKER:
    raise Exception("KAFKA_BROKER must be set")


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    with open(INPUT_FILE) as f:
        messages = json.load(f)

    for message in messages:
        payload = json.dumps(message).encode('utf-8')
        r = producer.send('tenant1_measurements', payload)
        logging.info(f"Sent {message['id']}")
        sleep(0.1)

    logging.info("All messages sent")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
