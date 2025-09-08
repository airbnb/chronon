# Orchestrators for Batch flows
## Airflow operator
```python
from airflow import DAG
from datetime import datetime
from ai.chronon.workflow.spark_submit import ChrononSparkSubmitOperator
from ai.chronon.repo.types import GroupBy, Aggregation, Operation, Window, TimeUnit

default_args = {
"owner": "airflow",
"start_date": datetime(2025, 2, 16),
"retries": 1
}

dag = DAG("chronon_spark_job", default_args=default_args, schedule_interval="@daily")

# Define a Chronon Thrift GroupBy object
group_by_job = GroupBy(
    sources=["datasetA"],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="purchase_amount", operation=Operation.SUM),
        Aggregation(input_column="purchase_amount", operation=Operation.LAST, windows=[Window(7, TimeUnit.DAYS)])
    ],
    online=True
)

chronon_task = ChrononSparkSubmitOperator(
    task_id="chronon_spark_submit",
    thrift_obj=group_by_job,
    application='ai.chronon:spark_uber_2.12:0.0.8',
    conf={
        'spark.jars.packages': 'ai.chronon:spark_uber_2.12:0.0.8'  # Ensure this package is available
    },
    dag=dag
)
```

## Dagster pipeline
```python
from dagster import Definitions
from ai.chronon.api.ttypes import GroupBy, Aggregation, Operation, Window, TimeUnit
from ai.chronon.workflow.spark_asset import chronon_spark_job

# Define a Chronon Thrift GroupBy object
group_by_job = GroupBy(
    sources=["datasetA"],
    keys=["user_id"],
    aggregations=[
        Aggregation(input_column="purchase_amount", operation=Operation.SUM),
        Aggregation(input_column="purchase_amount", operation=Operation.LAST, windows=[Window(7, TimeUnit.DAYS)])
    ],
    online=True
)

defs = Definitions(
    assets=[chronon_spark_job],
    resources={
        "thrift_obj": group_by_job
    }
)

```
