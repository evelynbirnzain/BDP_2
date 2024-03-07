import dask.dataframe as dd
import dask_mongo
import logging
import sys

filepath = 'data/ajtu-isnz.csv'
tenant = 'tenant2'
host = 'localhost'
port = 27017
logfile = 'logs/clientbatchingestapp_tenant2.log'

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(logfile)])

logger = logging.getLogger()


def ingest_file(filepath, tenant, host, port):
    df = dd.read_csv(filepath, assume_missing=True)
    logger.info(f"Read {len(df)} rows from {filepath}")

    col_names = df.columns
    df = df.iloc[:, df.isnull().sum() > 0]
    logger.info(f"Dropped {len(col_names) - len(df.columns)} empty columns")
    logger.info(f"Dropped columns: {set(col_names) - set(df.columns)}")
    logger.info(f"Remaining columns: {df.columns}")

    logger.info(f"Converting to json")
    bag = df.to_bag()
    bag = bag.map(lambda x: dict(zip(col_names, x)))

    logger.info(f"Writing to coredms")
    dask_mongo.to_mongo(bag, tenant, 'measurements', connection_kwargs={'host': host, 'port': port})


if __name__ == "__main__":
    ingest_file(filepath, tenant, host, port)
