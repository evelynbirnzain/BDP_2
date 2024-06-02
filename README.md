# Assignment 2: Big Data Platform

This repository contains a prototypical multi-tenant big data platform implementation that can run both batch and
stream ingestion pipelines for its tenants and includes basic monitoring and alerting capabilities. All data is
ingested into a central data management system powered by MongoDB.

## Overview

The current architecture in shown in the figure below. A detailed report can be found in
the [report](reports/Assignment-2-Report.md).

![current](reports/current.png)

### Stream ingestion

* Tenant producers: Sensors or other data sources that publish data to the messaging system.
* Messaging system: Central message broker powered by Kafka.
* Streamingest app: Consumes data from the messaging system and ingests it into the core data management system in near
  real-time.
* Streamingest manager: Responsible for starting and stopping the streamingest apps and enforcing the tenant service
  agreements.
* Streamingest monitor: Monitors the streamingest apps and alerts the streamingest manager if needed.

### Batch ingestion

* Client staging input directory: Directory where the client ingestors place the data to be ingested.
* Batchingest app: Consumes data from the client staging input directory and ingests it into the core data management
  system in batch mode.
* Batchingest manager: Responsible for starting and stopping the batchingest apps and enforcing the tenant service
  agreements.

## Deployment

Detailed deployment instructions can be found in the [deployment report](reports/Assignment-2-Deployment.md).