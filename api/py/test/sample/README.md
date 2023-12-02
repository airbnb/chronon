# Quickstart Guide

This guide walks you through the steps to create a training dataset with Chronon, using a fabricated underlying raw dataset.

Includes:
- Example implementation of the main API components for defining features - `GroupBy` and `Join`.
- The workflow for authoring these entities.
- The workflow for backfilling training data.

Does not include:
- A deep dive on the various concepts and terminologies in Chronon. For that, please see the [Introductory](https://chronon-ai.pages.dev/Introduction) documentation.
- Running streaming jobs and online serving of data (this only covers offline training data).

## Introduction

In this example, let's assume that we're a large online retailer, and we've detected a fraud vector based on users making purchases and later returning items. As such, we want to train a model that will be called when the item purchase flow commences, that will predict whether this transaction is likely to result in a fraudulent return.

## Raw data sources

Fabricated raw data is included in the (data)[data/] subdirectory within this dir. It includes three tables:

1. Users - includes basic information about users such as account created date
2. Purchases - a log of all purchases by users
3. Returns - a log of all returns made by users

## Setup
One time steps to get up and running with Chronon.

### 1. Setup the chronon repo
```shell
cd ~/repos
git clone git@github.com:airbnb/chronon.git
export PYTHONPATH=/Users/$USER/repos/chronon/api/py/:/Users/$USER/repos/chronon/api/py/test/sample/:$PYTHONPATH
```

#### 2. Download and setup spark (assuming you have a jdk already setup)

```shell
cd ~
curl -O https://archive.apache.org/dist/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
tar xf spark-3.2.4-bin-hadoop3.2.tgz
export SPARK_SUBMIT_PATH=/Users/$USER/spark-3.2.4-bin-hadoop3.2/bin/spark-submit
export SPARK_LOCAL_IP="127.0.0.1"
```

#### 3. Now switch to the config repo (within the project)

This is where we will do the bulk of development iterations from:
```shell
cd api/py/test/sample
```

## Chronon Development

Now that the setup steps are complete, we can start creating and testing various Chronon objects to define transformation and aggregations, and generate data.

### Step 1 - Define some features

Let's start with three feature sets, built on top of each of our three input sources.

1. Purchases features - We can aggregate the purchases log data to the user level, to give us a view into this user's previous activity on our platform. Specifically, we can compute `SUM`s `COUNT`s and `AVERAGE`s of their previous purchase amounts over various windows. See the (purchases GroupBy)[group_bys/purchases.py] for this definition.
2. Returns data - Similar to purchases, we can perform various aggregations about a user's previous returns. See the (returns GroupBy)[group_bys/returns.py].
3. User features - We can extract simple features about the user such as the date that their account was created, and whether their email is verified or not. See the (users GroupBy)[group_bys/users.py] for this definition.

### Step 2 - Join them together and backfill data

Next, we need the features that we previously defined backfilled and available in a single view for model training. This can be achieved using the Join API.

For our use case, it's very important that features are computed as of the correct timestamp. Because our model runs when the checkout flow begins, we'll want to be sure to use the corresponding timestamp in our backfill.

`Join` is the API that drives feature backfills for training data. It primarilly performs two functions:

1. Combines many features together into a wide view (hence the name Join).
2. Defines the primary keys and timestamps for which feature backfills should be performed. Chronon can then guarantee that feature values are correct as of this timestamp.
3. Performs scalabe backfills

See the (training_set Join)[joins/training_set.py] for how this defined. The `left` side of the join is what defines the timestamps and primary keys for the backfill (notice that it is built on top of the `checkout` event, as dictated by our use case).

Once the join is defined, we compile it using this command:

```shell
PYTHONPATH=/Users/$USER/repos/chronon/api/py/:/Users/$USER/repos/chronon/api/py/test/sample/ python3 ~/repos/chronon/api/py/ai/chronon/repo/compile.py --conf=joins/quickstart/training_set.py
```

This converts it into a thrift definition that we can submit to spark with the following command:

```shell
mkdir ~/quickstart_output

DRIVER_MEMORY=1G EXECUTOR_MEMORY=1G EXECUTOR_CORES=2 PARALLELISM=10 MAX_EXECUTORS=1 \
python3 ~/repos/chronon/api/py/ai/chronon/repo/run.py --mode=backfill \
--conf=production/joins/quickstart/training_set.v1 \
--local-data-path ~/repos/chronon/api/py/test/sample/data --local-warehouse-location ~/quickstart_output \
--ds=2023-11-30
```

#### Access the data
You can now see the parquet data here.
```shell
tree -h ~/quickstart_output/data/quickstart_training_set_v1/
``` 

You can also query it using the spark sql shell:

```aidl
cd ~/quickstart_output 
~/spark-3.2.4-bin-hadoop3.2/bin/spark-sql
```

And then: 

```
spark-sql> SELECT * FROM default.quickstart_training_set_v1 LIMIT 100;
```

**Note that if you have a spark-sql session active, it will likely put a transaction lock on your local metastore which will cause an error if you try to run other spark jobs.** So just close the sql session if you want to go back and modify the join.

### Modifying / Iterating on a Join

Chronon aims to make it as easy as possible to iterate on Joins. The steps are outlined here:

1. Modify the GroupBy to add whatever field you want (or create a new GroupBy and add it to the join's `right_parts`)
2. Rerun the `compile` step to generate the compiled join file
3. Rerun the join computation step.

Schema changes and backfills are all handled for you under the hood. All you need to do is make your semantic modifications and rerun the job, and Chronon will figure out what needs to be backfilled.

For example, if you change the `left` side of your Join query, Chronon will recompute all of the data for that join (because the left side of the join controls the rows in the whole table).

However, if you simple modify one of the GroupBys, Chronon will only backfill the columns associated with that GroupBy, and will leverage existing computation for the rest of the data. It achieves this by utilising intermediate tables produced by each run of Join computation.

