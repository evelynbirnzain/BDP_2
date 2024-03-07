import json
import logging
import os
import sys
from time import sleep

import dotenv
from kafka import KafkaProducer

dotenv.load_dotenv()

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS')
INPUT_FILE = os.getenv('DATA_FILE')

print(KAFKA_BROKERS)
print(INPUT_FILE)

logfile = sys.argv[2] if len(sys.argv) > 2 else 'logs/sensor.log'
logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

if not KAFKA_BROKERS or not INPUT_FILE:
    raise Exception("KAFKA_BROKERS and DATA_FILE must be set")


def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)

    with open(INPUT_FILE) as f:
        messages = json.load(f)

    n = int(sys.argv[1]) if len(sys.argv) > 1 else len(messages)

    logging.info(f"Sending {n} messages")
    for message in messages[:n]:
        payload = json.dumps(message).encode('utf-8')
        r = producer.send('tenant1_measurements', payload)
        logging.info(f"Sent {message['id']}")
        sleep(1)

    logging.info("All messages sent")
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
