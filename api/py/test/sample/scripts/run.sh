#!/bin/bash
set -x
printf "\n=> OFFLINE BACKFILL\n\n"
# Compile a config.
compile.py --conf joins/quickstart/training_set.py --force-overwrite
# Run a backfill for a join.
run.py --conf production/joins/quickstart/training_set.v1

printf "\n=> Online Group bys\n\n"
# Compute some upload tables.
run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload --ds 2023-12-01
run.py --conf production/group_bys/quickstart/returns.v1 --mode upload --ds 2023-12-01
# Upload tables to KV Store
# run sbt assembly under quickstart/mongo-online-impl outside of docker container to build jars for this step
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*]  /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_returns_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
# Do a simple group by fetch
run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"user_id":"5"}'

printf "\n=> METADATA UPLOAD \n\n"
# Upload the join metadata.
run.py --mode metadata-upload --conf production/joins// 

printf "\n=> FETCH JOIN \n\n"
# Fetch the join (requires group by uploads to kv store + metadata upload)
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}'

printf "\n => Logging Table \n\n"
# Feed some fetches to populate log KV table
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}' 
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}' 
# Dump the logged KV table into hive.
spark-submit --class ai.chronon.quickstart.online.MongoLoggingDumper --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.chronon_log_table mongodb://admin:admin@mongodb:27017/?authSource=admin
# Compile a group by to compute the latest schema 
compile.py --conf group_bys/quickstart/schema.py
# Get the latest schema per name
run.py --mode backfill --conf production/group_bys/quickstart/schema.v1
# Build the flattened logged table
run.py --mode log-flattener --conf production/joins/quickstart/training_set.v2 --log-table default.chronon_log_table --schema-table default.quickstart_schema_v1

printf "\n => STATS \n\n"
# Build the backfill table
run.py --mode backfill --conf production/joins/quickstart/training_set.v2
# Run stats on the features
run.py --mode stats-summary --conf production/joins/quickstart/training_set.v2
# Upload to KV Store
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_logged_daily_stats_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
# Fetch from KV Store
run.py --mode fetch --type join-stats --name quickstart/training_set.v2 -k '{"startTs":"0","endTs":"180000000000000"}'

printf "\n => LOGSTATS \n\n"
# Run Log stats
run.py --mode log-summary --conf production/joins/quickstart/training_set.v2
# Upload to KV Store
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_daily_stats_upload mongodb://admin:admin@mongodb:27017/?authSource=admin

printf"\n => OOC \n\n"
# Compute consistency metrics tables
run.py --mode consistency-metrics-compute --conf production/joins/quickstart/training_set.v2
# Upload consistency metrics to KV Store
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_training_set_v2_consistency_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
