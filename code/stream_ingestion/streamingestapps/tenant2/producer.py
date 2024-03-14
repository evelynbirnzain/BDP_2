import logging
import os
import sys
from time import sleep

import dotenv
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER')
INPUT_FILE = 'data/taxi-data-25.csv'

logfile = f"logs/producer_tenant2_{os.getppid()}.log"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

if not KAFKA_BROKER:
    raise Exception("KAFKA_BROKER must be set")


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

    with open(INPUT_FILE) as f:
        lines = f.readlines()
        lines = lines[1:]

        for line in lines:
            producer.send('tenant2_measurements', line.encode('utf-8'))
            logging.info(f"Sent {line.split(',')[0]}")
            sleep(0.1)

    logging.info("All messages sent")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
