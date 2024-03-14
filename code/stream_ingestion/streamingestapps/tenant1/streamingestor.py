import logging
import os
import json
import sys
import time

import pymongo
import dotenv

from kafka import KafkaConsumer, KafkaProducer

from code.stream_ingestion.streamingestapps.metrics import Metrics, set_interval

dotenv.load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
MONGO_URL = os.getenv('MONGO_URL')

if not KAFKA_BROKER or not MONGO_URL:
    raise Exception("KAFKA_BROKER and MONGO_URL must be set as environment variables")

if len(sys.argv) < 3:
    raise Exception("TENANT and KAFKA_TOPIC must passed as command line arguments")

TENANT = sys.argv[1]
KAFKA_TOPIC = sys.argv[2]

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout),
                              logging.FileHandler(f'logs/streamingestapp_{TENANT}_{os.getppid()}.log')])

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id="ingestor")
monitoring_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)

db_client = pymongo.MongoClient(MONGO_URL)
db = db_client[TENANT]


def report_metrics(metrics):
    report = metrics.generate_report()
    logging.info(f"Report: {report}")
    monitoring_producer.send("metrics", json.dumps(report).encode('utf-8'))


def main():
    logging.info("Starting ingestion")

    metrics = Metrics(tenant_id=TENANT)
    metrics.start()

    set_interval(lambda: report_metrics(metrics), 10)

    for message in consumer:
        start = time.time()

        data = json.loads(message.value)

        sensor = data.pop('sensor')
        sensor_ref = db['sensors'].find_one({"id": sensor['id']})
        if not sensor_ref:
            sensor_ref = db['sensors'].insert_one(sensor).inserted_id
        else:
            sensor_ref = sensor_ref['_id']

        data.pop('sampling_rate')

        data['sensor'] = sensor_ref
        db['measurements'].insert_one(data)

        end = time.time()
        metrics.update(sys.getsizeof(data), end - start)

    logging.info("Ingestion complete")

    consumer.close()
    db_client.close()


if __name__ == "__main__":
    main()
