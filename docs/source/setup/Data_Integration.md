# Data Integration

This section covers how to setup Chronon so that it can read the raw data in your warehouse for feature computation, and write out computed feature values for storage.

This is different than integrating with your online datastore for feature serving. For that, please refer to [Online Integration](./Online_Integration.md).

## Requirements

Chronon jobs require Spark to run. If you already have a spark environment up and running that is connected to your Data Warehouse, then integration should be very simple.

## Configuring Spark

To configure Chronon to run on spark, you just need a `spark_submit.sh` script that can be used in Chronon's [`run.py`](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/repo/run.py) Python script (this is the python-based CLI entry point for all jobs).

We recommend putting your `spark_submit.sh` within a `scripts/` subdirectory of your main `chronon` directory (see [Developer Setup docs](./Developer_Setup.md) for how to setup the main `chronon` directory.). If you do that, then you can use `run.py` as-is, as that is the [default location](https://github.com/airbnb/chronon/blob/main/api/py/ai/chronon/repo/run.py#L483) for `spark_submit.sh`.

You can see an example `spark_submit.sh` script used by the quickstart guide here: [Quickstart example spark_submit.sh](https://github.com/airbnb/chronon/blob/main/api/py/test/sample/scripts/spark_submit.sh).

Note that this replies on an environment variable set in the `docker-compose.yml` which basically just points `$SPARK_SUBMIT` variable to the system level `spark-submit` binary.

You can use the above script as a starting point for your own script.
