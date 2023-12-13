## Quickstart for Chronon demo


### Requirements

Docker

### Getting Started

To build the containers run `docker-compose up` in the repo root and let it run in a terminal window (when you kill this
process all the data generated should get cleaned and containers will stop).

To get started iterating with chronon in a new terminal window run `docker-compose exec bash main` to enter a shell environment with all the
requirements set up. If you want to automatically run all steps there's a run.sh script.


### Step 1: Run your first backfill

One first functionality to explore with Chronon is creating point in time correct feature values. Explore the
configuration for the feature definitions and join (view over the features with query timestamps) by looking at the
folders in `group_bys/quickstart/` and `joins/quickstart` respectively. To compute a table with the required features:
1. Load some initial tables into your local spark warehouse in the container `spark-shell -i scripts/data-loader.scala
   --master local`
2. Compile a couple of joins: `compile.py --conf joins/quickstart/training.py`
3. Run your first backfill: `run.py --conf production/joins/quickstart/training.v1`

You have now created a table that contains the feature values as for the timestamps defined in your left.
You can explore the table by running `spark-shell --master local`.

### Step 2: Upload batch data to your KVStore

It's time to move to the online world. We want to be able to query the features in different clients. For this we need
to upload batch data into a KVStore.
0. Compile your online jar: from the repo root go to quickstart/mongo-online-impl and run `sbt assembly`.
1. Run your first batch upload KV table: `run.py --mode upload --conf production/group_bys/quickstart/purchases.v1 --ds  2023-12-01`
2. Upload this table into MongoDB: `spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin`
3. Fetch data from the KVStore: `run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"user_id":"5"}'`

Feel free to explore more on the data being uploaded by looking at the onlineImpl provided in the quickstart directory
as well as the resulting upload KV table that can be viewed in spark-shell.

### Step 3: Fetch a join.

Follow step 2 for all your group bys. In the case of `training_set.v2` this includes purchases.v1 and returns.v1.

1. Run the metadata-upload job for the joins. `run.py --mode metadata-upload --conf production/joins//`
2. Fetch the join: `run.py --mode fetch --type join --name quickstart/training_set.v2 -k '{"user_id":"5"}'`


### Future Extensions

There are other flows to explore with Chronon.

1. Build a log table based on the fetched data.
1. Compute online offline consistency.
1. Stream data from a host and compute a realtime feature.
1. Build derived features.
1. Chain two joins together.
1. Bootstrap your feature with log data.
1. Compute stats on the features including drift.
