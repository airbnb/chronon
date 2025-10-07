# affirm/group_bys/paybright/repayment_data_v1_0.py

from ai.chronon.api.ttypes import Source, EntitySource, MetaData
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# This source uses a StagingQuery to automatically handle S3 YYYY/MM/DD partitioning.
# No manual view creation needed - Chronon handles everything automatically!

from ai.chronon.staging_query import StagingQuery
from ai.chronon.utils import get_staging_query_output_table_name

# StagingQuery automatically converts your S3 partitioning to Chronon's expected format
paybright_staging_query = StagingQuery(
    query="""
        SELECT *, 
               CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) as ds
        FROM (
          SELECT *, 
                 INPUT_FILE_NAME() as file_path,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 1) as year,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 2) as month,
                 REGEXP_EXTRACT(INPUT_FILE_NAME(), '.*/([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})/.*', 3) as day
          FROM parquet.`s3://affirm-risk-sherlock-ca/feature-store/paybright_repayment_data/v1`
          WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
        )
    """,
    startPartition="2025-01-01",  # Adjust to your data start date
    metaData=MetaData(
        name="paybright_repayment_data_v1",
        team="affirm",
        outputNamespace="affirm"
    )
)

paybright_snapshot_src = Source(
    entities=EntitySource(
        snapshotTable=get_staging_query_output_table_name(paybright_staging_query.v1),   # ← Auto-generated table
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
            )
        ),
    )
)

# Passthrough snapshot → no aggregations
paybright_repayment_data_v1_0 = GroupBy(
    sources=[paybright_snapshot_src],
    keys=["user_phone_number"],
    aggregations=None,
)
