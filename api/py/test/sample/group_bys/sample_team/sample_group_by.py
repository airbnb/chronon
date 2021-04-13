"""
Sample group by
"""
from ai.zipline.shorthand import *
from ai.zipline.utils import get_staging_query_output_table_name
from staging_queries.sample_team import sample_staging_query

v1 = GroupBy(
    name="sample_group_by",
    table="sample_namespace.{}".format(get_staging_query_output_table_name(sample_staging_query.v1)),
    model=DataModel.ENTITIES,
    keys=[("s2CellId", "s2CellId"), ("place_id", "place_id")],
    start_partition="2021-03-01",
    aggs=[
        Agg(column="impressed_unique_count_1d", op=SUM),
        Agg(column="viewed_unique_count_1d", op=SUM),
    ],
    production=False,
    tableProperties={
        "sample_config_json": """{"sample_key": "sample_value"}""",
        "description": "sample description"
    },
    outputNamespace="sample_namespace",
)
