import logging
import os
import json
import sys
import dotenv
from kafka import KafkaConsumer
import requests

dotenv.load_dotenv()
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS')
STREAM_INGEST_MANAGER_URL = os.getenv('STREAMINGESTMANAGER_URL')

print(KAFKA_BROKERS)
print(STREAM_INGEST_MANAGER_URL)

if not KAFKA_BROKERS or not STREAM_INGEST_MANAGER_URL:
    raise Exception("KAFKA_BROKERS and STREAMINGESTMANAGER_URL must be set as environment variables")

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(
    f'logs/streamingestmonitor.log')])

consumer = KafkaConsumer("metrics", bootstrap_servers=KAFKA_BROKERS, group_id="monitor")


def main():
    logging.info("Starting the monitoring")

    for message in consumer:
        data = json.loads(message.value)
        logging.info(f"Received montoring report: {data}")

        average_ingestion_time = float(data['metrics']['average_ingestion_time'][:-1])

        if average_ingestion_time > 0.001:
            requests.post(f"{STREAM_INGEST_MANAGER_URL}/alerts", json=data)
            logging.info(f"Sent alert for {data['origin']['tenant']}")

    logging.info("Monitoring complete")
    consumer.close()


if __name__ == "__main__":
    main()
