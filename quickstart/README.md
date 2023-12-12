## Quickstart for Chronon demo


### Requirements

Docker

### Getting Started

To build the containers run `docker-compose up` in the repo root.
To get started iterating with chronon run `docker-compose exec bash main` to enter a shell environment with all the
requirements set up.


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
1. Run your first batch upload KV table: `run.py --mode upload --conf production/group_bys/quickstart/purchases.v1`
2. Upload this table into MongoDB: `spark-submit ...`
3. Fetch data from the KVStore: `run.py --mode fetch --type group-by --name quickstart`

Feel free to explore more on the data being uploaded by looking at the onlineImpl provided in the quickstart directory
as well as the resulting upload KV table that can be viewed in spark-shell.


### Future Extensions

There are other flows to explore with Chronon.

1. Build a log table based on the fetched data.
1. Compute online offline consistency.
1. Stream data from a host and compute a realtime feature.
1. Build derived features.
1. Chain two joins together.
1. Bootstrap your feature with log data.
1. Compute stats on the features including drift.
