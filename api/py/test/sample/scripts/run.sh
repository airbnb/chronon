#!/bin/bash
set -x
printf "\n=> LOADING DATA INTO LOCAL HIVE\n\n"
spark-shell -i scripts/data-loader.scala --master local[*]
printf "\n=> COMPILING\n\n"
compile.py --conf joins/quickstart/training_set.py --force-overwrite
printf "\n=> OFFLINE BACKFILL\n\n"
run.py --conf production/joins/quickstart/training_set.v1
printf "\n=> ONLINE UPLOAD TABLE CREATION\n\n"
run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload --ds 2023-12-01
run.py --conf production/group_bys/quickstart/returns.v1 --mode upload --ds 2023-12-01
printf "\n=> UPLOAD TABLE TO MONGODB\n\n"
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_returns_v1_upload mongodb://admin:admin@mongodb:27017/?authSource=admin
printf "\n=> FETCH GROUP BY\n\n"
run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"user_id":"5"}' --version 0.0.56

printf "\n=> METADATA UPLOAD \n\n"
run.py --mode metadata-upload --conf production/joins// --version 0.0.56
printf "\n=> FETCH JOIN \n\n"
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}' --version 0.0.56

printf "\n => Logging Table \n\n"
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}' --version 0.0.56
run.py --mode fetch --name quickstart/training_set.v2 -k '{"user_id":"5"}' --version 0.0.56
spark-submit --class ai.chronon.quickstart.online.MongoLoggingDumper --master local[*] /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.chronon_log_table mongodb://admin:admin@mongodb:27017/?authSource=admin
compile.py --conf group_bys/quickstart/schema.py
run.py --mode backfill --conf production/group_bys/quickstart/schema.v1
run.py --mode log-flattener --conf production/joins/quickstart/training_set.v2 --log-table default.chronon_log_table --schema-table default.quickstart_schema_v1 --version 0.0.58

printf "\n => STATS \n\n"
run.py --mode backfill --conf production/joins/quickstart/training_set.v2
run.py --mode stats-summary --conf production/joins/quickstart/training_set.v2 --version 0.0.58
run.py --mode log-summary --conf production/joins/quickstart/training_set.v2 --version 0.0.58
