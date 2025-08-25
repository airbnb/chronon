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

import os

import ai.chronon.api.ttypes as api
from ai.chronon.utils import get_staging_query_output_table_name, chronon_root_path
from ai.chronon.repo import teams, TEAMS_FILE_PATH


def _resolve_staging_query_table_and_namespace(staging_query: api.StagingQuery):
    if staging_query.metaData.outputNamespace and staging_query.metaData.name:
        table_name = f"{staging_query.metaData.outputNamespace}.{staging_query.metaData.name}"
        source_namespace = staging_query.metaData.outputNamespace
    else:
        table_name = get_staging_query_output_table_name(staging_query, full_name=True)
        # Resolve namespace from team config if table_name contains template
        if "{{ db }}" in table_name and staging_query.metaData.name:
            team_name = staging_query.metaData.name.split(".")[0]
            teams_path = os.path.join(chronon_root_path, TEAMS_FILE_PATH)
            try:
                source_namespace = teams.get_team_conf(teams_path, team_name, "namespace")
            except ValueError:
                source_namespace = teams.get_team_conf(teams_path, "default", "namespace")
            table_name = table_name.replace("{{ db }}", source_namespace)
        else:
            source_namespace = table_name.split(".")[0]
    return table_name, source_namespace


def StagingQueryEventSource(
    staging_query: api.StagingQuery,
    query: api.Query,
    is_cumulative: bool = False
) -> api.Source:
    table_name, _ = _resolve_staging_query_table_and_namespace(staging_query)

    event_source = api.EventSource(
        table=table_name,
        query=query,
        isCumulative=is_cumulative,
        # No topic - staging query views are batch-only
        topic=None
    )

    source = api.Source(events=event_source)

    return source


def StagingQueryEntitySource(
    staging_query: api.StagingQuery,
    query: api.Query
) -> api.Source:
    table_name, _ = _resolve_staging_query_table_and_namespace(staging_query)

    # Create the EntitySource
    entity_source = api.EntitySource(
        snapshotTable=table_name,
        query=query,
        # No mutation table or topic - staging query views are batch-only
        mutationTable=None,
        mutationTopic=None
    )

    source = api.Source(entities=entity_source)

    return source


def get_staging_query_dependencies(staging_query: api.StagingQuery, lag: int = 0):
    table_name, source_namespace = _resolve_staging_query_table_and_namespace(staging_query)

    dependency_spec = f"{source_namespace}.chronon_signal_partitions/ds={{{{ ds }}}}/table_name={table_name}"

    return [dependency_spec]
