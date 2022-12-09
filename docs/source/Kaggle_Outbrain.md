#### 0. Setup chronon repo
```shell
cd ~/repos
git clone git@github.com:airbnb/chronon.git
export PYTHONPATH=/Users/$USER/repos/chronon/api/py/:/Users/$USER/repos/chronon/api/py/test/sample/:$PYTHONPATH
```

#### 1. Download and setup spark (assuming you have a jdk already setup)
```shell
cd ~
curl -O https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz
tar xf spark-2.4.8-bin-hadoop2.7.tgz
export SPARK_SUBMIT_PATH=/Users/$USER/spark-2.4.8-bin-hadoop2.7/bin/spark-submit
```

#### 2. Build local jar - with the csv loader
```shell
# From the root of the project
sbt spark_uber/assembly
```

#### 3. Now switch to the config repo (within the project)
This is where we will do the bulk of development iterations from
```shell
cd api/py/test/sample/
```

#### Compile the staging query
```shell
python3 ~/repos/chronon/api/py/ai/chronon/repo/compile.py --input_path=staging_queries/kaggle/outbrain.py
```

#### Compile the join 
```shell
PYTHONPATH=/Users/nikhil_simha/repos/chronon/api/py/:/Users/nikhil_simha/repos/chronon/api/py/test/sample/ \
python3 ~/repos/chronon/api/py/ai/chronon/repo/compile.py --conf=joins/kaggle/outbrain.py
```

### Run the join
```shell
DRIVER_MEMORY=16G EXECUTOR_MEMORY=16G EXECUTOR_CORES=6 PARALLELISM=100 MAX_EXECUTORS=1 \
python3 ~/repos/chronon/api/py/ai/chronon/repo/run.py --mode=backfill \
--chronon-jar=/Users/$USER/repos/chronon/spark/target/scala-2.11/spark_uber-assembly-csv_tool-0.0.18-SNAPSHOT.jar \
--conf=production/joins/kaggle/outbrain.training_set \
--local-data-path ~/kaggle_outbrain --local-warehouse-location ~/kaggle_outbrain_parquet \
--ds=2016-07-01 --step-days=1
```

### You can find the resulting parquet data
```shell
tree -h ~/kaggle_outbrain_parquet/data/kaggle_outbrain_training_set/
```