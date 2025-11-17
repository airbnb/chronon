# affirm/group_bys/paybright/repayment_data_v1_0.py

from ai.chronon.api.ttypes import Source, EntitySource, MetaData, Aggregation, Operation, Window, TimeUnit
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# This source uses a StagingQuery to automatically handle S3 YYYY/MM/DD partitioning.
# No manual view creation needed - Chronon handles everything automatically!

from ai.chronon.utils import get_staging_query_output_table_name
from staging_queries.affirm.paybright_staging_query import v1 as paybright_staging_query

paybright_snapshot_src = Source(
    entities=EntitySource(
        snapshotTable=get_staging_query_output_table_name(paybright_staging_query),   # ← Auto-generated table
        query=Query(
            selects=select(
                # entity key
                "user_phone_number",
                # timestamps
                "snapshot_time",
                # features
                "galactus__paybright__user__history_average_lateness_days__v1",
                "galactus__paybright__user__history_average_zeroed_lateness_days__v1",
                "galactus__paybright__user__history_days_since_last_payment__v1",
                "galactus__paybright__user__history_max_lateness_days__v1",
                "galactus__paybright__user__history_num_outstanding_loans__v1",
                "galactus__paybright__user__history_num_payments_last_60d__v1",
                "galactus__paybright__user__history_prop_fully_paid_off_loans__il__v1",
                "galactus__paybright__user__history_prop_fully_paid_off_loans__sp__v1",
                "galactus__paybright__user__history_total_payment_amount_cents_60d__v1",
                "galactus__paybright__user__history_total_payment_amount_cents__v1",
            ),
            time_column="snapshot_time"  # Required for windowed aggregations
        ),
    )
)

# Add meaningful aggregations for repayment data analysis
paybright_repayment_data_v1_0 = GroupBy(
    sources=[paybright_snapshot_src],
    keys=["user_phone_number"],
    aggregations=[
        # Average lateness metrics
        Aggregation(
            inputColumn="galactus__paybright__user__history_average_lateness_days__v1",
            operation=Operation.AVERAGE,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        Aggregation(
            inputColumn="galactus__paybright__user__history_average_zeroed_lateness_days__v1",
            operation=Operation.AVERAGE,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Max lateness
        Aggregation(
            inputColumn="galactus__paybright__user__history_max_lateness_days__v1",
            operation=Operation.MAX,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Days since last payment
        Aggregation(
            inputColumn="galactus__paybright__user__history_days_since_last_payment__v1",
            operation=Operation.MIN,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)]
        ),
        
        # Outstanding loans count
        Aggregation(
            inputColumn="galactus__paybright__user__history_num_outstanding_loans__v1",
            operation=Operation.COUNT,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Payment frequency
        Aggregation(
            inputColumn="galactus__paybright__user__history_num_payments_last_60d__v1",
            operation=Operation.SUM,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)]
        ),
        
        # Payment success rates
        Aggregation(
            inputColumn="galactus__paybright__user__history_prop_fully_paid_off_loans__il__v1",
            operation=Operation.AVERAGE,
            windows=[Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        Aggregation(
            inputColumn="galactus__paybright__user__history_prop_fully_paid_off_loans__sp__v1",
            operation=Operation.AVERAGE,
            windows=[Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Payment amounts
        Aggregation(
            inputColumn="galactus__paybright__user__history_total_payment_amount_cents_60d__v1",
            operation=Operation.SUM,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)]
        ),
        Aggregation(
            inputColumn="galactus__paybright__user__history_total_payment_amount_cents__v1",
            operation=Operation.SUM,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
    ],
)
