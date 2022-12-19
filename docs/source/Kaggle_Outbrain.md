# Kaggle Outbrain Example

This doc walks you through the steps to create a training dataset with Chronon for the [Outbrain Click Prediction](https://www.kaggle.com/c/outbrain-click-prediction) project on Kaggle.

Includes:
- Example implementation of the main API components for defining features - GroupBy and Join.
- An overview of the workflow for authoring and testing these objects. 

Does not include:
- A deep dive on the various concepts and terminologies in Chronon. For that, please see the [Introductory](https://chronon-ai.pages.dev/Introduction) documentation.

## Setup
One time steps to get up and running with Chronon.

#### 0. Setup the chronon repo
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

## Chronon Development

Now that the setup steps are complete, we can start creating and testing various Chronon objects to define transformation and aggregations, and generate data.

### Step 0 - Come up with some feature ideas

Before we start creating Chronon objects, we need to have some ideas for what features we want to experiment with.

Browsing the raw data available is generally a good starting point. For this example, we essentially have a [dataset of user behavior](https://www.kaggle.com/competitions/outbrain-click-prediction/data), with user clicks being one potentially interesting source of raw data.

A possible transformations that we might consider:
- The number of clicks per ad in the last N days.
- The number of clicks per ad in the last N days by various dimensions (per user, per platform, per geo).
- The average clickthru rate on each ad in the last N days.
- The average clickthru rate on each ad in the last N days by various dimensions (per user, per platform, per geo).

Note that these are basically variations of the same feature. They're all aggregating click data by using the SUM, COUNT, and AVERAGE operations, and different sets of primary keys (user, platform, geo).

In the following sections, we'll define the various chronon objects that are needed to run

### Sidenote - why use Chronon?

You might be looking at the above and thinking that you could simply write an SQL query to compute these aggregations. So why use Chronon at all?
1. **Time based aggregations:** In this case, each click has an associated timestamp, and we would want each time based aggregation (like average clickthru rate in the last three days) to be precisely as of that time. This is possible to do with window function in SQL, but they're complex and error prone, and if you get the timestamp wrong then you will train on data that is inconsistent with the distribution that your model at production inference time. In the worst case, this can also cause label leakage, which will result in dramatically worse model performance in production than expected based off training.
2. **Model Deployment (in the real world):** If this were actually a model that you wanted to deploy to production, you would need the same data that you used for training to be available in the serving environment. This poses a problem if you wrote a big complex analytical query with lots of complex window functions, as you probably cannot run this against any production database. Not only that, but for event based features like this, you likely also need realtime updates to your online feature values to ensure that your model actually performs as well as you expect, so batch computing and uploading online feature values isn't a great option either.

Chronon helps you these challenges in the following ways:
1. It offers a simple API for defining time based aggregations, and guarantees correct computation for each row of training data.
2. You define features once, and Chronon offers APIs for generating training data, and fetching your data online with low latency and streaming updates.

There are also a number of other benefits, such as discoverability, feature sharing, observability and governance.

### Step 1 - Create and run a Staging Query

Because we have a normalized view of the data, a good first step is join things together so that we can get the relevant primary keys onto the click data (specifically things like the user, device and geo information that we want to aggregate clicks by).  

To do this, we'll write a simple SQL join, and define it as a Staging Query. You can see the code [here](../../api/py/test/sample/staging_queries/kaggle/outbrain.py).

Sometimes you won't need to create a Staging Query if your data is sufficiently denormalized, and your raw data already has the relevant fields.

See more detailed documentation on Staging Query [here](https://chronon-ai.pages.dev/Introduction#staging-query).

#### Compile the staging query

Compiling takes the python that we wrote and turns it into a thrift serialized object that is runnable.

```shell
python3 ~/repos/chronon/api/py/ai/chronon/repo/compile.py --input_path=staging_queries/kaggle/outbrain.py
```

#### Run the staging query

Now that we have our compiled file, we can pass it into the `run.py` runner which will execute the SQL and produce an output table for us to use next. 
```shell
DRIVER_MEMORY=4G EXECUTOR_CORES=6 EXECUTOR_MEMORY=18G PARALLELISM=10 MAX_EXECUTORS=1 \
python3 ~/repos/chronon/api/py/ai/chronon/repo/run.py --mode=backfill \
--chronon-jar=/Users/$USER/repos/chronon/spark/target/scala-2.11/spark_uber-assembly-csv_tool-0.0.18-SNAPSHOT.jar \
--conf=production/staging_queries/kaggle/outbrain.base_table \
--local-data-path ~/kaggle_outbrain --local-warehouse-location ~/kaggle_outbrain_parquet
```

### Step 2 - Create some GroupBys

GroupBys are the primary API for creating features in Chronon. Each one is a set of features that share a data source and a primary key.

You can see the Code for GroupBys [here](../../api/py/test/sample/group_bys/kaggle/outbrain.py).

See detailed documentation on GroupBy [here](https://chronon-ai.pages.dev/Introduction#groupby).

**Note: we don't need to run the GroupBys individually**. While there is an API for doing so, in this case it's easier to skip ahead to the Join, which combines multiple GroupBys together into one dataset, and test that directly.

### Step 3 - Create and run a Join

As the name suggests, the main purpose of a join is to combine multiple GroupBys together into a single data source.

You can see the Code for the join [here](../../api/py/test/sample/joins/kaggle/outbrain.py).

See detailed documentation on Join [here](https://chronon-ai.pages.dev/Introduction#join).

#### Compile the join

Again, compiling creates a runnable serialized file out of your python definition.
```shell
PYTHONPATH=/Users/nikhil_simha/repos/chronon/api/py/:/Users/nikhil_simha/repos/chronon/api/py/test/sample/ \
python3 ~/repos/chronon/api/py/ai/chronon/repo/compile.py --conf=joins/kaggle/outbrain.py
```

#### Run the join

Running the join will backfill a training dataset with each of the features values computed correctly for each row defined on the `left` side of the join.
```shell
DRIVER_MEMORY=16G EXECUTOR_MEMORY=16G EXECUTOR_CORES=6 PARALLELISM=100 MAX_EXECUTORS=1 \
python3 ~/repos/chronon/api/py/ai/chronon/repo/run.py --mode=backfill \
--chronon-jar=/Users/$USER/repos/chronon/spark/target/scala-2.11/spark_uber-assembly-csv_tool-0.0.18-SNAPSHOT.jar \
--conf=production/joins/kaggle/outbrain.training_set \
--local-data-path ~/kaggle_outbrain --local-warehouse-location ~/kaggle_outbrain_parquet \
--ds=2016-07-01 --step-days=1
```

#### Access the data
You can now see the parquet data here.
```shell
tree -h ~/kaggle_outbrain_parquet/data/kaggle_outbrain_training_set/
``` 

### Step 4 - Model Training (out of scope for this demo)

Steps 1-3 conclude the data generation using Chronon. Next, you would take the resulting Join data and move on to model training.
