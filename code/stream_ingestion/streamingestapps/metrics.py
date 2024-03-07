import datetime
import os
import threading


class Metrics:
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

    def __init__(self, tenant_id):
        self.tenant_id = tenant_id

    def start(self):
        self.metrics['measurement_start_time'] = datetime.datetime.now()

    def generate_report(self):
        report = {
            "metrics": {
                "processing_rate": f"{round(self.metrics['processing_rate'], 2)} msg/s",
                "throughput": f"{round(self.metrics['throughput'], 2)} B/s",
                "average_ingestion_time": f"{round(self.metrics['average_ingestion_time'], 10)}s",
                "total_ingestion_data_size": f"{round(self.metrics['total_ingestion_data_size'] / (1024 ** 2), 2)} MB",
                "total_number_of_messages": self.metrics['total_number_of_messages']
            },
            "timeframe": {
                "start_time": self.metrics['measurement_start_time'].strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": self.metrics['measurement_end_time'].strftime("%Y-%m-%d %H:%M:%S")
            },
            "origin": {
                "tenant": self.tenant_id,
                "streaming_app_id": os.getppid(),
                "streaming_app_name": "streamingestor"
            }
        }

        return report

    def update(self, data_size, new_ingestion_time):
        self.ingestion_times.append(new_ingestion_time)
        self.metrics['total_ingestion_data_size'] += data_size
        self.metrics['total_number_of_messages'] += 1
        self.metrics['measurement_end_time'] = datetime.datetime.now()
        self.metrics['average_ingestion_time'] = sum(self.ingestion_times) / len(self.ingestion_times)
        self.metrics['processing_rate'] = 1 / self.metrics['average_ingestion_time']
        self.metrics['throughput'] = self.metrics['total_ingestion_data_size'] / (
                    self.metrics['measurement_end_time'] - self.metrics['measurement_start_time']).total_seconds()


def set_interval(func, sec):
    def func_wrapper():
        set_interval(func, sec)
        func()

    t = threading.Timer(sec, func_wrapper)
    t.start()
    return t
