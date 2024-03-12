import os
import time
import datetime
import logging
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import dotenv
from concurrent.futures import ProcessPoolExecutor as Pool
import subprocess
import pathlib

if not os.path.exists('logs/batchingestion_metrics.log'):
    with open('logs/batchingestion_metrics.log', 'w') as f:
        f.write("tenant,file,file_size,ingestion_start_time,ingestion_end_time,ingestion_time,success\n")

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(), logging.FileHandler('logs/batchingestmanager.log')])

metrics = logging.getLogger('metrics')
metrics.setLevel(logging.INFO)
metrics.addHandler(logging.FileHandler('logs/batchingestion_metrics.log'))

dotenv.load_dotenv()
PYTHON = os.getenv('PYTHON_EXECUTABLE')

if not PYTHON:
    raise Exception("PYTHON_EXECUTABLE must be set as environment variable")

BATCHINGESTAPPS_DIR = 'code/batch_ingestion/batchingestapps'

running = {}

def on_complete(tenant, file, file_size, ingestion_start_time, success):
    end_time = datetime.datetime.now()
    ingestion_time = end_time - ingestion_start_time
    metrics.info(f"{tenant},{file},{file_size},{ingestion_start_time},{end_time},{ingestion_time},{success}")
    logging.info(f"Ingestion of {file} complete in {ingestion_time}")
    running[tenant] -= 1
    if success:
        logging.info(f"Removing {file}")
        os.remove(file)
    else:
        logging.error(f"Ingestion of {file} failed")


def ingest_file(event):
    logging.info(f"Event: {event.event_type} {event.src_path}")
    if event.is_directory:
        logging.info(f"New tenant directory added to staging directory: {event.src_path}")
    else:
        logging.info(f"New file uploaded: {event.src_path}")
        src_path = pathlib.Path(event.src_path)
        tenant = src_path.parts[-3]
        subdir = src_path.parts[-2]

        config_path = pathlib.Path(BATCHINGESTAPPS_DIR, tenant, 'config.yaml')
        logging.info(f"Checking config for {tenant} at {config_path}")
        if not os.path.exists(config_path):
            logging.error(f"No config found for {tenant}")
            return

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        logging.info(f"Config: {config}")
        logging.info(f"Subdir: {subdir}")

        if subdir not in config['executables']:
            logging.error(
                f"Subdir {subdir} does not have an executable for {tenant}. Registered: {config['executables']}")
            return

        filetype = src_path.suffix[1:].upper()
        if filetype not in config['data_constraints']['file_types']:
            logging.error(
                f"Filetype {filetype} not supported for {tenant}. Supported filetypes: {config['data_constraints']['file_types']}")
            return

        filesize = os.path.getsize(event.src_path)
        max_filesize = int(config['data_constraints']['max_file_size'][:-2]) * 1024 * 1024
        if filesize > max_filesize:
            logging.error(
                f"File {event.src_path} is too large ({filesize} > {config['data_constraints']['max_file_size']})")
            return

        if tenant in running and running[tenant] >= config['service_agreement']['max_concurrent_ingestions']:
            logging.error(
                f"Too many concurrent ingestions for {tenant}. Max: {config['service_agreement']['max_concurrent_ingestions']}")
            return

        executable = pathlib.Path(BATCHINGESTAPPS_DIR, tenant, config['executables'][subdir])
        if os.path.exists(executable):
            logging.info(f"Executing {executable}")
            running[tenant] = 1 if tenant not in running else running[tenant] + 1
            cmd = f'{PYTHON} {executable} "{src_path}"'
            f = pool.submit(subprocess.call, cmd, shell=True)
            f.add_done_callback(
                lambda f: on_complete(tenant, src_path, filesize, datetime.datetime.now(), f.result() == 0))
        else:
            logging.error(f"Executable {executable} does not exist")


class EventHandler(FileSystemEventHandler):
    def on_created(self, event):
        ingest_file(event)


if __name__ == "__main__":
    pool = Pool(max_workers=4)

    path = 'data/client-staging-input-directories'
    logging.info(f"Watching {path}")

    event_handler = EventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(5)
    finally:
        observer.stop()
        observer.join()
