#!/bin/bash
set -x
spark-shell -i scripts/data-loader.scala --master local[*]
compile.py --conf joins/quickstart/training_set.py
run.py --conf production/joins/quickstart/training_set.v1
run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload
run.py --conf production/group_bys/quickstart/returns.v1 --mode upload
spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] --jars /srv/jars/mongo-spark-connector_2.12-3.0.2.jar /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar default.quickstart_purchases_v1_upload mongodb://admin:admin@mongodb:27017/admin.chronon_batch
