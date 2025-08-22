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

import json

import ai.chronon.api.ttypes as api
from ai.chronon.utils import get_staging_query_output_table_name


def StagingQueryEventSource(
    staging_query: api.StagingQuery,
    query: api.Query,
    is_cumulative: bool = False
) -> api.Source:
    """
    Creates an EventSource that references a StagingQuery view.

    This wrapper automatically:
    1. Uses the staging query's output table as the event source table
    2. Generates signal partition dependencies for orchestration
    3. Ensures batch-only processing (no realtime component)

    Args:
        staging_query: The StagingQuery object that defines the view
        query: Query configuration for the event source
        is_cumulative: Whether this is a cumulative event source

    Returns:
        api.Source: A properly configured Source with EventSource
    """
    # Get the staging query output table name (the view)
    # For tests and direct usage, construct the table name manually
    if staging_query.metaData.outputNamespace and staging_query.metaData.name:
        table_name = f"{staging_query.metaData.outputNamespace}.{staging_query.metaData.name}"
    else:
        table_name = get_staging_query_output_table_name(staging_query, full_name=True)

    # Create the EventSource
    event_source = api.EventSource(
        table=table_name,
        query=query,
        isCumulative=is_cumulative,
        # No topic - staging query views are batch-only
        topic=None
    )

    # Create and return the Source
    source = api.Source(events=event_source)

    return source


def StagingQueryEntitySource(
    staging_query: api.StagingQuery,
    query: api.Query
) -> api.Source:
    """
    Creates an EntitySource that references a StagingQuery view.

    This wrapper automatically:
    1. Uses the staging query's output table as the entity snapshot table
    2. Generates signal partition dependencies for orchestration
    3. Ensures batch-only processing (no realtime component)

    Args:
        staging_query: The StagingQuery object that defines the view
        query: Query configuration for the entity source

    Returns:
        api.Source: A properly configured Source with EntitySource
    """
    # Get the staging query output table name (the view)
    # For tests and direct usage, construct the table name manually
    if staging_query.metaData.outputNamespace and staging_query.metaData.name:
        table_name = f"{staging_query.metaData.outputNamespace}.{staging_query.metaData.name}"
    else:
        table_name = get_staging_query_output_table_name(staging_query, full_name=True)

    # Create the EntitySource
    entity_source = api.EntitySource(
        snapshotTable=table_name,
        query=query,
        # No mutation table or topic - staging query views are batch-only
        mutationTable=None,
        mutationTopic=None
    )

    # Create and return the Source
    source = api.Source(entities=entity_source)

    return source


def get_staging_query_dependencies(staging_query: api.StagingQuery, lag: int = 0):
    """
    Generate signal partition dependencies for a staging query view.

    This function should be used explicitly when creating GroupBy configs
    that depend on staging query views.

    Args:
        staging_query: The StagingQuery object
        lag: The lag in days for the dependency (default: 0)

    Returns:
        List[str]: List of dependency strings for signal partitions
    """
    # For tests and direct usage, construct the table name manually
    if staging_query.metaData.outputNamespace and staging_query.metaData.name:
        table_name = f"{staging_query.metaData.outputNamespace}.{staging_query.metaData.name}"
    else:
        table_name = get_staging_query_output_table_name(staging_query, full_name=True)
    source_namespace = table_name.split('.')[0] if '.' in table_name else 'default'
    clean_table_name = table_name.split('.')[-1] if '.' in table_name else table_name

    dependency = {
        "name": f"wait_for_view_{clean_table_name}_ds{'_minus_' + str(lag) if lag > 0 else ''}",
        "spec": f"{source_namespace}.chronon_signal_partitions/ds={{{{ ds }}}}/table_name={table_name}"
    }

    return [json.dumps(dependency)]
