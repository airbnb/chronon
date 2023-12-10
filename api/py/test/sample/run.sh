#!/bin/bash
set -x
spark-shell -i scripts/data-loader.scala --master local[*]
compile.py --conf joins/quickstart/training_set.py
run.py --conf production/joins/quickstart/training_set.v1
