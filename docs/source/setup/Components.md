# Components

This is a high level overview of the various components of Chronon and how they fit together.

## Chronon directory

This is a code directory where users come to create features and author various other Chronon entities. These entities are all python code, and also have a corollary thrift file that is created in the same directory as a result of the `compile` command. See the [Test](../test_deploy_serve/Test.md) and [Deploy](../test_deploy_serve/Deploy.md) documentation for more details.

## Batch compute jobs

Compiled Chronon definitions can be used to run batch compute jobs using the Chronon engine. There are a number of purposes that these jobs can serve:

1. Create training data, written out to materialized tables
2. Compute feature values to upload to the [online KV store](#online-kv-store)
3. Compute data quality and online/offline consistency metrics
4. Analyze a feature definition to gather high level metadata about schemas and hotkeys in source data

## Streaming Jobs

Chronon features that are built on top of a streaming source have an associated streaming job which listens to raw events in the stream and uses them to update feature values in the online KV store.

## Online KV store

This is the database that powers online serving of features.

## Fetcher

This is a library that can be used to fetch feature values for production inference with low latency. There are both Scala and Java implementations. It can also be used to create a fetcher service that can be called via REST. It pulls data from the online KV store, and also emits metrics and logs.

## Materialized tables

Chronon outputs data to the warehouse for a number of use cases, the main one being training data. This data is saved in your data warehouse as a Hive table.

## Online/Offline consistency

This a core chronon guarantee that the data you train your model on is consistent with the data that you fetch at model inference time. See the [Source](../authoring_features/Source.md) and [Join](../authoring_features/Join.md) documentation in authoring features for more concrete examples of how this works in practice.

## Airflow Orchestration

Compiled Chronon objects will often have an associated job or DAG to run their computation on some schedule. This can be either to keep values up to date in the KV store, create new training data on a regular basis, or run data quality checks.
