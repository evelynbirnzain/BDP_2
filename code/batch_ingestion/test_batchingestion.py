import shutil
import time
import os
import logging
import pandas as pd

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler(), logging.FileHandler('logs/batchingestion_performancetest.log')])

target_dir_lc = "data/client-staging-input-directories/tenant1/taxi_data"
target_dir_hc = "data/client-staging-input-directories/tenant2/taxi_data"
log_file = "logs/batchingestapp_tenant2.log"

""" For all of the constraint "tests" it should be verified that there is an appropriate log entry in the log file.
    The respective log entries can be found both in the 'logs/batchingestapp_tenant1.log' and 
    'logs/batchingestion_metrics.log' files.
    
    1. Filetype JSON not supported for tenant1. Supported filetypes: ['CSV']
    2. File data\\client-staging-input-directories\\tenant2\\taxi_data\\taxi-data.csv is too large (426477039 > 300MB).
    3. Too many concurrent ingestions for tenant2. Max: 2 Current: 2.
    4. Too much data in staging directory. Allowed: 536870912.0, Current: 639623530.

"""

if not os.path.exists(target_dir_lc):
    os.makedirs(target_dir_lc)
if not os.path.exists(target_dir_hc):
    os.makedirs(target_dir_hc)


def write(size, lines):
    with open(f"data/taxi-data-{size}.csv", 'w') as f:
        f.writelines(lines[0])
        for i in range(size // 25):
            f.writelines(lines[1:])


if not os.path.exists("data/taxi-data-100.csv") or not os.path.exists("data/taxi-data-300.csv") or not os.path.exists(
        "data/taxi-data-400.csv"):
    with open("data/taxi-data-25.csv", 'r') as f:
        lines = f.readlines()
        write(100, lines)
        write(300, lines)
        write(400, lines)


def test_constraints():
    logging.info("Start testing constraints")

    # test illegal file type (allowed: CSV)
    logging.info("Testing illegal file type")
    shutil.copy("data/sensor-data.json", f"{target_dir_lc}/test.json")

    # test too large file size (400MB > 350MB)
    logging.info("Testing too large file size")
    shutil.copyfile("data/taxi-data-400.csv", f"{target_dir_hc}/taxi-data.csv")

    wait_for_processing(target_dir_hc)

    # test too many concurrent ingestions (allowed: 2)
    logging.info("Testing too many concurrent ingestions")
    for i in range(3):
        shutil.copyfile(f"data/taxi-data-100.csv", f"{target_dir_hc}/taxi-data-{i}.csv")

    wait_for_processing(target_dir_hc)

    # test not enough storage space (2 >=300MB files in with 500MB staging storage)
    logging.info("Testing not enough storage space in staging directory")
    for i in range(2):
        shutil.copyfile(f"data/taxi-data-300.csv", f"{target_dir_hc}/taxi-data-2-{i}.csv")


def test_performance():
    logging.info("Start testing performance")
    for num_concurrent in [1, 2, 4, 8, 12, 16, 20, 24]:
        logging.info(f"Testing {num_concurrent} concurrent ingestions")
        reps = 1
        if num_concurrent < 8:
            reps = 8 // num_concurrent
        for _ in range(reps):
            for i in range(num_concurrent):
                shutil.copyfile(f"data/taxi-data-100.csv", f"{target_dir_lc}/taxi-data-{i}.csv")
            for i in range(num_concurrent):
                shutil.copyfile(f"data/taxi-data-100.csv", f"{target_dir_hc}/taxi-data-{i}.csv")
            wait_for_processing(target_dir_lc)
            wait_for_processing(target_dir_hc)
            df = pd.read_csv("logs/batchingestion_metrics.log")
            if 'concurrent_ingestions' not in df.columns:
                df['concurrent_ingestions'] = pd.NA
            df['concurrent_ingestions'] = df['concurrent_ingestions'].fillna(num_concurrent)
            df.to_csv("logs/batchingestion_metrics.log", index=False)


def wait_for_processing(target_dir):
    while len(os.listdir(target_dir)) > 1:
        time.sleep(5)


if __name__ == "__main__":
    test_performance()
    test_constraints()
