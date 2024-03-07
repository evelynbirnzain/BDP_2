import logging
import os
import json
import sys
import time
import datetime

import pymongo
import dotenv

from kafka import KafkaConsumer, KafkaProducer

import threading


def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()

    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t


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

metrics = {
    "processing_rate": 0.0,
    "throughput": 0.0,
    "average_ingestion_time": 0.0,
    "total_ingestion_data_size": 0.0,
    "total_number_of_messages": 0,
    "measurement_start_time": datetime.datetime.now(),
    "measurement_end_time": datetime.datetime.now()
}

ingestion_times = []


def report_metrics():
    report = {
        "metrics": {
            "processing_rate": f"{round(metrics['processing_rate'], 2)} msg/s",
            "throughput": f"{round(metrics['throughput'], 2)} B/s",
            "average_ingestion_time": f"{round(metrics['average_ingestion_time'], 10)}s",
            "total_ingestion_data_size": f"{round(metrics['total_ingestion_data_size'] / (1024 ** 2), 2)} MB",
            "total_number_of_messages": metrics['total_number_of_messages']
        },
        "timeframe": {
            "start_time": metrics['measurement_start_time'].strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": metrics['measurement_end_time'].strftime("%Y-%m-%d %H:%M:%S")
        },
        "origin": {
            "tenant": TENANT,
            "streaming_app_id": os.getppid(),
            "streaming_app_name": "streamingestor"
        }
    }
    logging.info(f"Report: {report}")

    monitoring_producer.send("metrics", json.dumps(report).encode('utf-8'))


def main():
    logging.info("Starting ingestion")
    metrics['measurement_start_time'] = datetime.datetime.now()

    set_interval(report_metrics, 10)
    for message in consumer:
        start = time.time()

        data = json.loads(message.value)
        collection = db[KAFKA_TOPIC]
        r = collection.insert_one(data)

        end = time.time()
        ingestion_times.append(end - start)

        metrics['total_ingestion_data_size'] += sys.getsizeof(data)
        metrics['total_number_of_messages'] = len(ingestion_times)
        metrics['measurement_end_time'] = datetime.datetime.now()

        metrics['average_ingestion_time'] = sum(ingestion_times) / len(ingestion_times)

        duration = metrics['measurement_end_time'] - metrics['measurement_start_time']
        metrics['processing_rate'] = metrics['total_number_of_messages'] / duration.total_seconds()
        metrics['throughput'] = metrics['total_ingestion_data_size'] / duration.total_seconds()

    logging.info("Ingestion complete")

    consumer.close()
    db_client.close()


if __name__ == "__main__":
    main()
