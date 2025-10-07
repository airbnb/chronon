 # affirm/group_bys/instrument_risk/instrument_risk_data_v1_0.py

from ai.chronon.api.ttypes import Source, EntitySource, MetaData, Aggregation, Operation, Window, TimeUnit
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

# This source uses a StagingQuery to automatically handle S3 YYYY/MM/DD partitioning.
# No manual view creation needed - Chronon handles everything automatically!

from ai.chronon.utils import get_staging_query_output_table_name
from staging_queries.affirm.instrument_risk_staging_query import v1 as instrument_risk_staging_query

instrument_risk_snapshot_src = Source(
    entities=EntitySource(
        snapshotTable=get_staging_query_output_table_name(instrument_risk_staging_query),   # ← Auto-generated table
        query=Query(
            selects=select(
                # entity key
                "user_ari",
                # timestamps
                "snapshot_time",
                "processed_time",
                # features
                "model_name",
                "model_version",
                "galactus__predict_pay__instrument_aris__v1",
                "galactus__predict_pay__instrument_risk_scores__v1",
            ),
            time_column="snapshot_time"  # Required for windowed aggregations
        ),
    )
)

# Add meaningful aggregations for instrument risk data analysis
instrument_risk_data_v1_0 = GroupBy(
    sources=[instrument_risk_snapshot_src],
    keys=["user_ari"],
    aggregations=[
        # Model version tracking
        Aggregation(
            inputColumn="model_version",
            operation=Operation.LAST,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)]
        ),
        
        # Risk score aggregations (for arrays, we'll use LAST for now)
        Aggregation(
            inputColumn="galactus__predict_pay__instrument_risk_scores__v1",
            operation=Operation.LAST,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Instrument ARIs tracking
        Aggregation(
            inputColumn="galactus__predict_pay__instrument_aris__v1",
            operation=Operation.LAST,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
        
        # Model name tracking
        Aggregation(
            inputColumn="model_name",
            operation=Operation.LAST,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS)]
        ),
        
        # Count of predictions over time
        Aggregation(
            inputColumn="snapshot_time",
            operation=Operation.COUNT,
            windows=[Window(7, TimeUnit.DAYS), Window(30, TimeUnit.DAYS), Window(90, TimeUnit.DAYS)]
        ),
    ],
)
