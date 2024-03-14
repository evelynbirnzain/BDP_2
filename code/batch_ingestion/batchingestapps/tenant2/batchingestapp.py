import os

import dask.dataframe as dd
import dask_mongo
import logging
import sys
import dotenv
import pathlib

dotenv.load_dotenv()

MONGO_URL = os.getenv('MONGO_URL')

if not MONGO_URL:
    raise Exception("MONGO_URL must be set as environment variable")

HOST, PORT = MONGO_URL.split('//')[1].split(':')
PORT = int(PORT)

FILEPATH = pathlib.Path(sys.argv[1])
TENANT = FILEPATH.parts[-3]

logfile = f"logs/batchingestapp_{TENANT}.log"
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

logger = logging.getLogger()
logger.info(f"Starting ingestion of {FILEPATH} for {TENANT}")


def ingest_file(file_path: str, tenant: str):
    df = dd.read_csv(file_path, assume_missing=True)
    logger.info(f"Read {len(df)} rows from {file_path}")

    col_names = df.columns
    df = df.iloc[:, df.isnull().sum() > 0]
    logger.info(f"Dropped {len(col_names) - len(df.columns)} empty columns")
    logger.info(f"Dropped columns: {set(col_names) - set(df.columns)}")
    logger.info(f"{len(df.columns)} columns remaining")

    logger.info(f"Converting to json")
    bag = df.to_bag()
    bag = bag.map(lambda x: dict(zip(col_names, x)))
    bag = bag.map(lambda x: {k: v for k, v in x.items() if v is not None})

    logger.info(f"Writing to coredms")
    dask_mongo.to_mongo(bag, tenant, 'measurements', connection_kwargs={'host': HOST, 'port': PORT}, compute=True)


if __name__ == "__main__":
    ingest_file(FILEPATH, TENANT)
