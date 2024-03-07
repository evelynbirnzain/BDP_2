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
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS')
MONGO_URL = os.getenv('MONGO_URL')

if not KAFKA_BROKERS or not MONGO_URL:
    raise Exception("KAFKA_BROKERS and MONGO_URL must be set as environment variables")

if len(sys.argv) < 3:
    raise Exception("TENANT and KAFKA_TOPIC must passed as command line arguments")

TENANT = sys.argv[1]
KAFKA_TOPIC = sys.argv[2]

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout),
                              logging.FileHandler(f'logs/streamingestapp_{TENANT}_{os.getppid()}.log')])

consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKERS, group_id="ingestor")
monitoring_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)

db_client = pymongo.MongoClient(MONGO_URL)
db = db_client[TENANT]


def report_metrics(metrics):
    report = metrics.generate_report()
    logging.info(f"Report: {report}")
    monitoring_producer.send("metrics", json.dumps(report).encode('utf-8'))


colnames = ["trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
            "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area", "fare",
            "tips", "tolls", "extras", "trip_total", "payment_type", "company", "pickup_centroid_latitude",
            "pickup_centroid_longitude", "pickup_centroid_location", "dropoff_centroid_latitude",
            "dropoff_centroid_longitude", "dropoff_centroid_location"]


def main():
    logging.info("Starting ingestion")

    metrics = Metrics(tenant_id=TENANT)
    metrics.start()

    set_interval(report_metrics(metrics), 10)

    for message in consumer:
        start = time.time()

        data = message.value.decode('utf-8')
        data = data.split(',')
        data = dict(zip(colnames, data))

        location = {
            "area": data.pop('pickup_community_area'),
            "latitude": data.pop('pickup_centroid_latitude'),
            "longitude": data.pop('pickup_centroid_longitude'),
            "location": data.pop('pickup_centroid_location')
        }

        data['location'] = location

        db['trips'].insert_one(data)

        end = time.time()
        metrics.update(sys.getsizeof(data), end - start)

    logging.info("Ingestion complete")

    consumer.close()
    db_client.close()


if __name__ == "__main__":
    main()
