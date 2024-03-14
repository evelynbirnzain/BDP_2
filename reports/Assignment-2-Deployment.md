# Deployment/installation guide

# Setup

## Install requirements

````shell
python -m venv venv
.\venv\Scripts\activate.bat # Windows
source venv/bin/activate # Unix
pip install -r code/requirements.txt
````

In `.env`, set the `PYTHON_EXECUTABLE` to the path of the Python executable in the virtual environment.

## Set up mock messaging system and coredms

````shell
docker-compose up
````

Other deployments for Kafka and MongoDB can be set up as well. In that case, adapt `KAFKA_BROKER` and `MONGO_URL`.

## Batch ingestion

Run the batch ingestion manager:

````shell
python .\code\batch_ingestion\batchingestmanager.py
````

It will watch for changes in `data/client-staging-input-directories`. Directories for two tenants are already set up.
Both of these tenants have registered an ingestor that handles CSV files for the `taxi_data` subdirectory. You can
manually test the setup by moving a CSV file to the `taxi_data` subdirectory of one of the tenants.

````shell
copy data/taxi-data-20.csv data/client-staging-input-directories/tenant1/taxi_data 
````

The batch ingest manager will schedule ingestion and remove the file when done. The general processing can be monitored
in the shell where the batch ingest manager is running; the logs are also in `logs/batch_ingestmanager.log`,
`batchingestapp_tenant[1|2].log`; some metrics in `logs/batchingestion_metrics.log`.

### Performance and constraint tests

The performance and constraint tests will generate some bigger data files to test the system with; setup and run the
tests with:

````shell
python .\code\batch_ingestion\test_batchingestion.py
````

The tests are semi-automated. You can verify that the batchingestmanager catches the constraints by
checking `logs/batchingestmanager.log` and `logs/batchingestion_metrics.log`.

## Near real-time ingestion

Partition the used Kafka topics to allow for parallel ingestion.

````shell
docker compose exec messagingsystem kafka-topics.sh --create --topic tenant1_measurements --partitions 32 --replication-factor 1 --bootstrap-server messagingsystem:9092
docker compose exec messagingsystem kafka-topics.sh --create --topic tenant2_trips --partitions 32 --replication-factor 1 --bootstrap-server messagingsystem:9092
````

Run the stream ingestion manager:

````shell
python .\code\stream_ingestion\streamingestmanager.py
````

Run the stream ingest monitor:

````shell
python .\code\stream_ingestion\streamingestmonitor.py
````

Start and stop ingestor apps with the client:

````shell
python .\code\stream_ingestion\streamingestmanager_client.py start [tenant1|tenant2]
python .\code\stream_ingestion\streamingestmanager_client.py stop {ingestor_id}
`````

`ingestor_id` is the id of the ingestor app that you want to stop; it will be
returned when starting the ingestor app.

Start a producer to send messages for the ingestors to consume:

````shell
python .\code\stream_ingestion\streamingestapps\[tenant1|tenant2]\producer.py
````
