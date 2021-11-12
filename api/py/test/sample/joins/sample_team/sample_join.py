"""
Sample Join
"""
from group_bys.sample_team import sample_group_by
from staging_queries.sample_team import sample_staging_query

from ai.zipline.api import ttypes as api
from ai.zipline.utils import get_staging_query_output_table_name

v1 = api.Join(
    left=api.Source(
        entities=api.EntitySource(
            snapshotTable="sample_namespace.{}".format(get_staging_query_output_table_name(sample_staging_query.v1)),
            query=api.Query(
                startPartition='2021-03-01',
            )
        )
    ),
    joinParts=[api.JoinPart(
        groupBy=sample_group_by.v1
    )],
    metaData=api.MetaData(
        name="sample_join",
        tableProperties={
            "config_json": """{"sample_key": "sample_value"}"""
        },
        outputNamespace="sample_namespace"
    )
)
