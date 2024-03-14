import shutil
import time
import os
import json

input_path = "data/Taxi_Trips__2013-2023__20240313.mZ6CgXoa.csv"
target_dir_t2 = "data/client-staging-input-directories/tenant2/taxi_data"
target_dir_t1 = "data/client-staging-input-directories/tenant1/taxi_data"
log_file = "logs/batchingestapp_tenant2.log"


""" For all of the "tests" it should be verified that there is an appropriate log entry in the log file.
    
    1. Filetype JSON not supported for tenant2. Supported filetypes: ['CSV']
    2. File data\\client-staging-input-directories\\tenant1\\taxi_data\\taxi-data.csv is too large (426477039 > 300MB).
    3. Too many concurrent ingestions for tenant1. Max: 2 Current: 2.
    4. Too much data in staging directory. Allowed: 536870912.0, Current: 639623530.

"""
def test_constraints():
    # test illegal file type (allowed: CSV)
    test_json = json.dumps({"a": 1})
    with open("data/test.json", "w") as f:
        f.write(test_json)
    shutil.move("data/test.json", f"{target_dir_t2}/test.json")

    # test too large file size (400MB > 350MB)
    shutil.copyfile(input_path, f"{target_dir_t1}/taxi-data.csv")

    wait_for_processing(target_dir_t1)

    # test too many concurrent ingestions (allowed: 2)
    for i in range(3):
        shutil.copyfile(f"data/taxi-data-100.csv", f"{target_dir_t1}/taxi-data-{i}.csv")

    wait_for_processing(target_dir_t1)

    # test not enough storage space (2 >=300MB files in with 500MB staging storage)
    for i in range(2):
        shutil.copyfile(f"data/taxi-data-300.csv", f"{target_dir_t1}/taxi-data-2-{i}.csv")


def test_performance():
    for num_concurrent in [1, 2, 4, 8, 16]:
        for i in range(num_concurrent):
            shutil.copy(input_path, f"{target_dir_t2}/taxi-data-{i}.csv")
        wait_for_processing(target_dir_t2)


def wait_for_processing(target_dir):
    while len(os.listdir(target_dir)) > 0:
        print(f"Waiting for {len(os.listdir(target_dir))} files to be ingested")
        time.sleep(5)


if __name__ == "__main__":
    # test_performance()
    test_constraints()
