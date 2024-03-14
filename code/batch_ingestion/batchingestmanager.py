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

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
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
    logging.info(f"Ingestions still running for this tenant: {running[tenant]}")

    if success:
        logging.info(f"Ingestion of {file} successful")
    else:
        logging.error(f"Ingestion of {file} failed")

    logging.info(f"Removing {file}.")
    try:
        os.remove(file)
    except Exception as e:
        logging.error(f"Failed to remove {file}: {e}")


def on_constraint_violation(tenant, file, message):
    logging.error(f"Constraint violation for {tenant}: {message}")
    metrics.info(f"{tenant},{file},{os.path.getsize(file)},,,,{message}")
    try:
        os.remove(file)
    except Exception as e:
        logging.error(f"Failed to remove {file}: {e}")


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
            on_constraint_violation(tenant, src_path, f"No config found for {tenant}")
            return

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # wait while file is being "uploaded" (only needed for the tests)
        fs = os.path.getsize(src_path)
        while True:
            time.sleep(1)
            fs_new = os.path.getsize(src_path)
            if fs == fs_new:
                break
            fs = fs_new

        total_size = sum(os.path.getsize(f) for f in src_path.parent.glob('**/*') if os.path.isfile(f))
        allowed = float(config['service_agreement']['storage_space_staging'][:-2]) * 1024 * 1024 * 1024
        if total_size > allowed:
            on_constraint_violation(tenant, src_path, f"Too much data in staging directory. Allowed: {allowed / 1024 / 1024} MB. Current: {total_size / 1024 / 1024} MB")
            return

        if subdir not in config['executables']:
            on_constraint_violation(tenant, src_path, f"Subdir {subdir} does not have an executable for {tenant}. Registered: {config['executables']}")
            return

        filetype = src_path.suffix[1:].upper()
        if filetype not in config['data_constraints']['file_types']:
            on_constraint_violation(tenant, src_path, f"Filetype {filetype} not supported for {tenant}. Supported filetypes: {config['data_constraints']['file_types']}")
            return

        filesize = os.path.getsize(src_path)
        max_filesize = int(config['data_constraints']['max_file_size'][:-2])
        if config['data_constraints']['max_file_size'].endswith('GB'):
            max_filesize *= 1024 * 1024 * 1024
        elif config['data_constraints']['max_file_size'].endswith('MB'):
            max_filesize *= 1024 * 1024

        logging.info(f"Filesize: {filesize}, Max filesize: {max_filesize}")
        if filesize > max_filesize:
            on_constraint_violation(tenant, src_path, f"File {src_path} is too large ({filesize} > {config['data_constraints']['max_file_size']}).")
            return

        if tenant in running and running[tenant] >= config['service_agreement']['max_concurrent_ingestions']:
            on_constraint_violation(tenant, src_path, f"Too many concurrent ingestions for {tenant}. Max: {config['service_agreement']['max_concurrent_ingestions']} Current: {running[tenant]}.")
            return

        executable = pathlib.Path(BATCHINGESTAPPS_DIR, tenant, config['executables'][subdir])
        if os.path.exists(executable):
            logging.info(f"Executing {executable}")
            running[tenant] = 1 if tenant not in running else running[tenant] + 1
            cmd = f'{PYTHON} {executable} "{src_path}"'
            start_time = datetime.datetime.now()
            f = pool.submit(subprocess.call, cmd, shell=True)
            f.add_done_callback(
                lambda f: on_complete(tenant, src_path, filesize, start_time, f.result() == 0))
        else:
            on_constraint_violation(tenant, src_path, f"No executable found for {subdir} in {tenant}")


class EventHandler(FileSystemEventHandler):
    def on_created(self, event):
        ingest_file(event)


if __name__ == "__main__":
    pool = Pool()

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
