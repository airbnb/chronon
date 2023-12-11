#!/bin/bash
set -x
spark-shell -i scripts/data-loader.scala --master local[*]
compile.py --conf joins/quickstart/training_set.py
run.py --conf production/joins/quickstart/training_set.v1
run.py --conf production/group_bys/quickstart/purchases.v1 --mode upload
java -cp /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar:/tmp/spark_uber_2.12-0.0.59-assembly.jar  ai.chronon.quickstart.online.Spark2MongoUpload default.quickstart_purchases_v1_upload  chronon_batch
