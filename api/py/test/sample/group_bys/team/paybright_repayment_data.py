from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# Create a GroupBy for paybright repayment data (pass-through)
paybright_repayment_data_v1 = GroupBy(
    sources=[
        Source(
            entities=EntitySource(
                snapshotTable="ml_ofs_galactus_materialize_paybright_repayment_data",  # Your table
                query=Query(
                    selects=select(
                        "user_id",
                        "snapshot_time",
                        "galactus_paybright_user__history_average_lateness_days_v1",
                        "galactus_paybright_user__history_average_zeroed_lateness_days_v1",
                        "galactus_paybright_user__history_max_lateness_days_v1",
                        "galactus_paybright_user__history_num_outstanding_loans_v1",
                        "galactus_paybright_user__history_num_payments_last_60d_v1",
                        "galactus_paybright_user__history_prop_fully_paid_off_loans_v1",
                        "galactus_paybright_user__history_total_payment_amount_cents_60d_v1",
                        "galactus_paybright_user__history_total_payment_amount_cents_v1"
                    ),
                ),
            ),
        ),
    ],
    keys=["user_id"],
    aggregations=None,  # No aggregations - just pass-through
)

print("GroupBy created successfully!")
