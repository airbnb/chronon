# Intro

Chronon can automatically generate spark pipelines from user configuration.

Chronon utilizes Hive and Kafka as data sources, but since we rely on Spark, it should be fairly easy to incorporate any spark supported data source.

We recommend integrating with airflow to schedule these pipelines. But any scheduler should be straight forward to integrate with.


![Architecture](./images/Overall%20Architecture.png)

## Pre-requisites


## Integrations

There are essentially four integration points:

- Repository - this is where your users will define chronon configurations. We recommend that this live in a repository where you other airflow pipelines live to make deployment easier. Once you have the repository setup you can begin using chronon for offline batch 

- For online Serving
    - KV Store - for storing and serving features in low latency. This can be any kv store that can support point write, point lookup, scan and bulk write.
    - Event decoding - for reading bytes from kafka and converting them into a Chronon Event or a Chronon Mutation. If you have a convention between how you convert data in kafka into data in warehouse, you would need to follow that same convention to decode as well.

- Airflow - for scheduling spark pipelines that backfill training.



### Repository Setup

mkdir

```scala

```



