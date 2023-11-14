
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

from ai.chronon import query
from ai.chronon.group_by import GroupBy, TimeUnit, Window
from ai.chronon.api.ttypes import EventSource, EntitySource, Aggregation, Operation, JoinPart

from ai.chronon.join import Join

ratings_features = GroupBy(
      sources=[
          EntitySource(
              snapshotTable="item_info.ratings_snapshots_table",
              mutationsTable="item_info.ratings_mutations_table",
              mutationsTopic="ratings_mutations_topic",
              query=query.Query(
                  selects={
                      "rating": "CAST(rating as DOUBLE)",
                  }))
      ],
      keys=["item"],
      aggregations=[Aggregation(
        operation=Operation.AVERAGE,
        windows=[Window(length=90, timeUnit=TimeUnit.DAYS)]),
      ])


view_features = GroupBy(
      sources=[
          EventSource(
              table="user_activity.user_views_table",
              topic="user_views_stream",
              query=query.Query(
                  selects={
                      "view": "if(context['activity_type'] = 'item_view', 1 , 0)",
                  },
                  wheres=["user != null"]))
      ],
      keys=["user", "item"],
      aggregations=[
          Aggregation(
            operation=Operation.COUNT,
            windows=[Window(length=5, timeUnit=TimeUnit.HOURS)]),
      ])

item_rec_features = Join(
    left=EventSource(
        table="user_activity.view_purchases",
        query=query.Query(
            start_partition='2021-06-30'
        )
    ),
    # keys are automatically mapped from left to right_parts
    right_parts=[
        JoinPart(groupBy=view_features),
        JoinPart(groupBy=ratings_features)
    ]
)
