# affirm/group_bys/instrument/instrument_risk_data_v1_0.py

from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# This GroupBy is a 1:1 translation of your Feast FeatureView:
#   - Uses snapshot data (no aggregations)
#   - Each record represents the instrument risk snapshot for a user at snapshot_time
#   - Columns map directly to the Feast Field() definitions

instrument_risk_source = Source(
    entities=EntitySource(
        # Replace with your actual table name or registered dataset path
        snapshotTable="instrument_risk_data.v1",
        query=Query(
            selects=select(
                "user_ari",  # entity key
                "snapshot_time",
                "model_name",
                "model_version",
                # Feast defines these as Array(String) and Array(Float64)
                # Chronon can store arrays as is if your table supports them
                "galactus__predict_pay__instrument_aris__v1",
                "galactus__predict_pay__instrument_risk_scores__v1",
            )
        ),
    )
)

instrument_risk_data_v1_0 = GroupBy(
    sources=[instrument_risk_source],
    keys=["user_ari"],  # entity key
    aggregations=None,  # passthrough (snapshot-style)
)
