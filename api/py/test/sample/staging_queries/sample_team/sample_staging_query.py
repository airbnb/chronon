
#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

from ai.chronon.api.ttypes import StagingQuery, MetaData

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
