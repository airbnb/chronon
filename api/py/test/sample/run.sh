#!/bin/bash
set -x
echo "\n=> LOADING DATA INTO LOCAL HIVE\n"
spark-shell -i scripts/data-loader.scala --master local[*]
echo "\n=> COMPILING\n"
compile.py --conf joins/quickstart/training_set.py
echo "\n=> OFFLINE BACKFILL\n"
run.py --conf production/joins/quickstart/training_set.v1
echo "\n=> ONLINE UPLOAD TABLE CREATION\n"
run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload
run.py --conf production/group_bys/quickstart/returns.v1 --mode upload
echo "\n=> UPLOAD TABLE TO MONGODB\n"
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] --jars /srv/jars/mongo-spark-connector_2.12-3.0.2.jar /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/admin.QUICKSTART_PURCHASES_V1_BATCH
echo "\n=> FETCH GROUP BY\n"
run.py --mode fetch --type group-by --name quickstart/purchases.v1 -k '{"purchase_id":"3167C678-A78C-F63D-27B9-8E7A6BA141BD"}'
