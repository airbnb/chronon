## Quickstart for Chronon demo


### Requirements

Docker

### Getting Started

To build the containers run `docker-compose up` in the repo root and let it run in a terminal window (when you kill this
process all the data generated should get cleaned and containers will stop).

To get started iterating with chronon in a new terminal window run `docker-compose exec bash main` to enter a shell environment with all the
requirements set up. If you want to automatically run all steps there's a run.sh script.

Some of the steps require an online jar. This is an implementation for communication with the KV Store. We provide an
example that uses mongoDB as the KV Store in quickstart/mongo-online-impl.


### Use case: Run your first backfill and create a point in time correct view of aggregations

One first functionality to explore with Chronon is creating point in time correct feature values. Explore the
configuration for the feature definitions and join (view over the features with query timestamps) by looking at the
folders in `group_bys/quickstart/` and `joins/quickstart` respectively. To compute a table with the required features:
1. Load some initial tables into your local spark warehouse in the container `spark-shell -i scripts/data-loader.scala
   --master local`
2. Compile a couple of joins: `compile.py --conf joins/quickstart/training.py`
3. Run your first backfill: `run.py --conf production/joins/quickstart/training.v1`

You have now created a table that contains the feature values as for the timestamps defined in your left.
You can explore the table by running `spark-shell --master local`.

### Use case: Upload batch data to your KVStore and fetch group by batch features

It's time to move to the online world. We want to be able to query the features in different clients. For this we need
to upload batch data into a KVStore.
0. Compile your online jar: from the repo root go to quickstart/mongo-online-impl and run `sbt assembly`.
1. Run your first batch upload KV table: `run.py --mode upload --conf production/group_bys/quickstart/purchases.v1 --ds  2023-12-01`
2. Upload this table into MongoDB: `spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin`
3. Fetch data from the KVStore: `run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"user_id":"5"}'`

Feel free to explore more on the data being uploaded by looking at the onlineImpl provided in the quickstart directory
as well as the resulting upload KV table that can be viewed in spark-shell.

### Use case: Fetch online features defined in a join.

Follow step 2 for all your group bys. In the case of `training_set.v2` this includes purchases.v1 and returns.v1.

1. Run the metadata-upload job for the joins. `run.py --mode metadata-upload --conf production/joins//`
2. Fetch the join: `run.py --mode fetch --type join --name quickstart/training_set.v2 -k '{"user_id":"5"}'`


### Use case: Build a logging table from fetches.

Once you submit a few fetches you may want to have a logged table to keep as history and possible to bootstrap your
join. To do this there are a few steps.

1. Dump the logged KV table into hive.
```
spark-submit --class ai.chronon.quickstart.online.MongoLoggingDumper --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.chronon_log_table mongodb://admin:admin@mongodb:27017/?authSource=admin
```
1. Compile a group by to compute the latest schema for the log encoded data. `compile.py --conf group_bys/quickstart/schema.py`
1. Build the schemas table: `run.py --mode backfill --conf production/group_bys/quickstart/schema.v1`
1. Build the flattened logged table `run.py --mode log-flattener --conf production/joins/quickstart/training_set.v2 --log-table default.chronon_log_table --schema-table default.quickstart_schema_v1`

### Use case: Compute stats and fetch them.

Stats are computed and stored in offline tables as well as uploaded into the KV Store so they can be fetched (and
compute the drifts). The steps are as follows:
1. Build the backfill table `run.py --mode backfill --conf production/joins/quickstart/training_set.v2`
1. Run stats on the features: `run.py --mode stats-summary --conf production/joins/quickstart/training_set.v2`
1. Upload to KV Store
```
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_logged_daily_stats_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
```
1. Fetch from KV Store `run.py --mode fetch --type join-stats --name quickstart/training_set.v2 --version 0.0.56 -k '{"startTs":"0","endTs":"180000000000000"}'`

Additionally you can compute stats on the log tables.
1. Run log stats `run.py --mode log-summary --conf production/joins/quickstart/training_set.v2`
1. Upload to KV Store
```
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_daily_stats_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
```

### Use case: Compute online offline consistency

Online offline consistency helps determine if stream lag is making an effect on the features being served. Chronon
provides an offline table that summarizes the differences between the "backfill" value of the log table against the
actual logs. There's also an upload table that can be uploaded to the KVStore for fetching.
1. Compute consistency metrics tables:
`run.py --mode consistency-metrics-compute --conf production/joins/quickstart/training_set.v2`
1. Upload consistency metrics to KV Store
```
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_consistency_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
```

### Future Extensions

There are other flows to explore with Chronon.

1. Stream data from a host and compute a realtime feature.
1. Build derived features.
1. Chain two joins together.
1. Bootstrap your feature with log data.
