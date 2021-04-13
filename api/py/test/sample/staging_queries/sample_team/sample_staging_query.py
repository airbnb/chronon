from ai.zipline.api.ttypes import StagingQuery, MetaData

query = """
SELECT
    id_listing,
    place_id,
    S2_CELL(lat, lng, 12) AS s2CellId,
    impressed_unique_count_1d,
    viewed_unique_count_1d,
    ds
FROM sample_namespace.sample_table
WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
"""

v1 = StagingQuery(
    query=query,
    startPartition="2020-03-01",
    setups=[
        "CREATE TEMPORARY FUNCTION S2_CELL AS 'com.sample.hive.udf.S2CellId'",
    ],
    metaData=MetaData(
        name='sample_staging_query',
        outputNamespace="sample_namespace",
        dependencies=["sample_namespace.sample_table/ds={{ ds }}"],
        tableProperties={
            "sample_config_json": """{"sample_key": "sample value}""",
        }
    )
)
