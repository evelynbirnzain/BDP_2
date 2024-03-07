import os

import dask.dataframe as dd
import dask_mongo
import logging
import sys
import dotenv

dotenv.load_dotenv()

MONGO_URL = os.getenv('MONGO_URL')

if not MONGO_URL:
    raise Exception("MONGO_URL must be set as environment variable")

HOST, PORT = MONGO_URL.split(':')

FILEPATH = sys.argv[1]
TENANT = sys.argv[1].split('/')[-2]

logfile = f"logs/batchingestapp_{TENANT}.log"
logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

logger = logging.getLogger()


def ingest_file():
    df = dd.read_csv(FILEPATH, assume_missing=True)
    logger.info(f"Read {len(df)} rows from {FILEPATH}")

    col_names = df.columns
    df = df.iloc[:, df.isnull().sum() > 0]
    logger.info(f"Dropped {len(col_names) - len(df.columns)} empty columns")
    logger.info(f"Dropped columns: {set(col_names) - set(df.columns)}")
    logger.info(f"Remaining columns: {df.columns}")

    logger.info(f"Converting to json")
    bag = df.to_bag()
    bag = bag.map(lambda x: dict(zip(col_names, x)))

    logger.info(f"Writing to coredms")
    dask_mongo.to_mongo(bag, TENANT, 'measurements', connection_kwargs={'host': HOST, 'port': PORT})


if __name__ == "__main__":
    ingest_file()
