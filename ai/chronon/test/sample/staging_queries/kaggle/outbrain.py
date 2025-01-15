
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

base_table = StagingQuery(
    query="""
        SELECT
            clicks_train.display_id,
            clicks_train.ad_id,
            clicks_train.clicked,
            events.document_id,
            events.uuid,
            events.platform,
            events.geo_location,
            CAST(events.timestamp as LONG) + 1465876799998 as ts,
            FROM_UNIXTIME((CAST(events.timestamp as LONG) + 1465876799998)/1000, 'yyyy-MM-dd') as ds
        FROM
            kaggle_outbrain.clicks_train as clicks_train
        JOIN
            kaggle_outbrain.events as events
        ON clicks_train.display_id = events.display_id
        AND ABS(HASH(clicks_train.display_id)) % 100  < 5
        AND ABS(HASH(events.display_id)) % 100  < 5
    """,
    metaData=MetaData(
        name='outbrain_left',
        outputNamespace="default",
    )
)
