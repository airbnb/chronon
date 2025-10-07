# affirm/group_bys/paybright/repayment_data_v1_0.py

from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# If your snapshot is registered as a Hive/Glue table, use that full name below.
# If it's just a parquet path, point a table/view at it first (or whatever your env uses).
#
# Notes:
# - snapshotTimeColumn matches Feast's `timestamp_field="snapshot_time"`
# - createdTimeColumn is optional; include if you want write-time lineage (Feast used `created_timestamp_column="processed_time"`)

paybright_snapshot_src = Source(
    entities=EntitySource(
        snapshotTable="paybright_repayment_data.v1",   # ← replace with your actual table (e.g., db.schema.table)
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
