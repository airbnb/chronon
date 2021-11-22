from staging_queries.sample_team import sample_staging_query

from ai.zipline.api import ttypes as api
from ai.zipline.group_by import GroupBy
from ai.zipline.utils import get_staging_query_output_table_name

v1 = GroupBy(
    sources=api.Source(
        entities=api.EntitySource(
            snapshotTable="sample_namespace.{}".format(get_staging_query_output_table_name(sample_staging_query.v1)),
            query=api.Query(
                startPartition='2021-03-01',
                selects={
                    'impressed_unique_count_1d': 'impressed_unique_count_1d',
                    'viewed_unique_count_1d': 'viewed_unique_count_1d',
                    's2CellId': 's2CellId',
                    'place_id': 'place_id'
                }
            )
        )
    ),
    keys=["s2CellId", "place_id"],
    aggregations=[
        api.Aggregation(
            inputColumn="impressed_unique_count_1d",
            operation=api.Operation.SUM
        ),
        api.Aggregation(
            inputColumn="viewed_unique_count_1d",
            operation=api.Operation.SUM
        ),
    ],
    production=False,
    table_properties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    output_namespace="sample_namespace",
)
