from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

# Define the source reading from MinIO S3
source = Source(
    events=EventSource(
        table="raw_data",
        query=Query(
            selects=select(
                "user_id",
                "purchase_price",
                "item_category",
                "ts"
            ),
            setups=[
                "CREATE TEMPORARY VIEW raw_data AS SELECT *, DATE_FORMAT(ts, 'yyyy-MM-dd') as ds FROM parquet.`s3a://chronon/warehouse/data/purchases/purchases.parquet`"
            ],
            time_column="ts"
        )
    )
)

# Define time windows
window_sizes = [
    Window(length=1, timeUnit=TimeUnit.DAYS),    # 1 day
    Window(length=7, timeUnit=TimeUnit.DAYS),    # 7 days
]

# Create the GroupBy configuration
v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    aggregations=[
        Aggregation(
            input_column="purchase_price",
            operation=Operation.SUM,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.COUNT,
            windows=window_sizes
        ),
        Aggregation(
            input_column="purchase_price",
            operation=Operation.AVERAGE,
            windows=window_sizes
        ),
    ],
    online=True,
    backfill_start_date="2023-01-01",
    output_namespace="bootcamp",
    table_properties={
        "location": "s3a://chronon/warehouse/bootcamp/user_purchase_features/"
    }
)
