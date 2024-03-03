# Assignment report

## Part 1 - Batch data ingestion pipeline (weighted factor for grades = 3)

1. The ingestion will be applied to files of data. Design a schema for a set of constraints for data files that `mysimbdp`
   will support the ingestion. Design a schema for a set of constraints for tenant service agreement. Explain why you,
   as a platform provider, decide such constraints. Implement these constraints into simple configuration files. Provide
   two examples (e.g., JSON or YAML) for two different tenants to specify constraints on service agreement and files for
   the tenant. (1 point)

   
2. Each tenant will put the tenant's data files to be ingested into a staging file directory (or a storage
   bucket), `client-staging-input-directory` within `mysimbdp` (the staging directory is managed by the platform). Each
   tenant provides its ingestion programs/pipelines, `clientbatchingestapp`, which will take the tenant's files as
   input, in `client-staging-input-directory`, and ingest the files into`mysimbdp-coredms`. Any `clientbatchingestapp`
   must perform at least one type of data wrangling to transform data elements in files to another structure for
   ingestion. As a tenant, explain the design of `clientbatchingestapp` and provide one implementation. Note
   that `clientbatchingestapp` follows the guideline of `mysimbdp` given in the next Point 3. (1 point)

3. The `mysimbdp` provider provisions an execution environment for running tenant's ingestion
   pipelines (`clientbatchingestapp`). As the `mysimbdp` provider, design and implement a
   component `mysimbdp-batchingestmanager` that invokes tenant's `clientbatchingestapp` to perform the ingestion for
   available files in `client-staging-input-directory`. `mysimbdp` imposes the model that `clientbatchingestapp` has to
   follow but `clientbatchingestapp` is, in principle, a blackbox to `mysimbdp-batchingestmanager`. Explain
   how `mysimbdp-batchingestmanager` knows the list of `clientbatchingestapp` and decides/schedules the execution
   of `clientbatchingestapp` for different tenants. (1 point)

4. Explain your design for the multi-tenancy model in `mysimbdp`: which parts of `mysimbdp` will be shared for all tenants,
   which parts will be dedicated for individual tenants so that you, as a platform provider, can add and remove tenants
   based on the principle of pay-per-use. Develop test `clientbatchingestapp`, test data, and test constraints of files,
   and test service profiles for tenants according to your deployment. Show the performance of ingestion tests,
   including failures and exceptions, for 2 different tenants in your test environment and constraints. Demonstrate
   examples in which data will not be ingested due to a violation of constraints. Present and discuss the maximum amount
   of data per second you can ingest in your tests. (1 point)

5. Implement and provide logging features for capturing successful/failed ingestion as well as metrics about ingestion
   time, data size, etc., for files which have been ingested into `mysimbdp`. Logging information must be stored in
   separate files, databases or a monitoring system for analytics of ingestion. Explain how `mysimbdp` could use such
   logging information. Show and explain simple statistical data extracted from logs for individual tenants and for the
   whole platform with your tests. (1 point)

## Part 2 - Near real-time data ingestion (weighted factor for grades = 3)

1. Tenants will put their data into messages and send the messages to a messaging system. `mysimbdp-messagingsystem` (
   provisioned by `mysimbdp`) and tenants will develop ingestion programs, `clientstreamingestapp`, which read data from
   the messaging system and ingest the data into `mysimbdp-coredms`. For near real-time ingestion, explain your design
   for the multi-tenancy model in `mysimbdp`: which parts of the `mysimbdp` will be shared for all tenants, which parts will
   be dedicated for individual tenants so that `mysimbdp` can add and remove tenants based on the principle of pay-
   per-use. (1 point)

2. Design and implement a component `mysimbdp-streamingestmanager`, which can start and stop `clientstreamingestapp`
   instances on-demand. `mysimbdp` imposes the model that `clientstreamingestapp` has to follow so
   that `mysimbdp-streamingestmanager` can invoke `clientstreamingestapp` as a blackbox. Explain the model w.r.t.
   steps and what the tenant has to do in order to write `clientstreamingestapp`. (1 point)

3. Develop test ingestion programs (`clientstreamingestapp`), which must include one type of data wrangling (
   transforming the received message to a new structure). Show the performance of ingestion tests, including failures
   and exceptions, for at least 2 different tenants in your test environment. Explain the data used for testing. (1
   point)

4. `clientstreamingestapp` decides to report its processing rate, including average ingestion time, total ingestion data
   size, and number of messages to `mysimbdp-streamingestmonitor` within a predefined period of time. Design the report
   format and explain possible components, flows and the mechanism for reporting. (1 point)
5. Implement a feature in `mysimbdp-streamingestmonitor` to receive the report from `clientstreamingestapp`. Based on
   the report from `clientstreamingestapp`, when the performance is below a threshold, e.g., average ingestion time is
   too low, `mysimbdp-streamingestmonitor` decides to inform `mysimbdp-streamingestmanager` about the situation.
   Implement a feature in `mysimbdp-streamingestmanager` to receive information informed
   by `mysimbdp-streamingestmonitor`. (1 point)

## Part 3 - Integration and Extension (weighted factor for grades = 1)

Notes: no software implementation is required for this part

1. Produce an integrated architecture, with a figure, for the logging and monitoring of both batch and near real-time
   ingestion features (Part 1, Point 5 and Part 2, Points 4-5). Explain how a platform provider could know the amount of
   data ingested and existing errors/performance for individual tenants. (1 point)
2. In the stream ingestion pipeline, assume that a tenant has to ingest the same data but to different sinks,
   e.g., `mysimbdp-coredms` for storage and a new `mysimbdp-streamdataprocessing` component. What features/solutions can
   you provide and recommend to your tenant? (1 point)
3. Assume that the tenant wants to protect the data during the ingestion by using some encryption mechanisms to encrypt
   data in files. Thus, `clientbatchingestapp` has to deal with encrypted data. Which features/solutions do you
   recommend to the tenants, and which services might you support for this goal? (1 point)
4. In the case of near real-time ingestion, assume that we want to (i) detect the quality of data to ingest only data
   with a predefined quality of data and (ii) store the data quality detected into the platform. Given your
   implementation in Part 2, how would you suggest a design/change for achieving this goal? (1 point)
5. Assume a tenant has multiple `clientbatchingestapp`. Each is suitable for a type of data and has different workloads,
   such as complex transformation or feature engineering (e.g., different CPUs, memory consumption and execution time).
   How would you extend your design and implementation in Part 1 (only explain the concept/design) to support this
   requirement? (1 point)