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
from ai.chronon.staging_query import (
    StagingQueryEventSource,
    get_staging_query_dependencies
)
from ai.chronon.group_by import GroupBy, Aggregation, Operation



def test_get_staging_query_dependencies():
    """Test explicit dependency generation helper function."""
    staging_query = api.StagingQuery(
        metaData=api.MetaData(
            name="test_deps_staging_query",
            outputNamespace="test_namespace"
        ),
        query=api.Query(
            selects={"user": "user", "value": "some_value"},
            startPartition="2023-01-01", 
            endPartition="2023-12-31"
        ),
        createView=True
    )
    
    # Test with no lag
    deps = get_staging_query_dependencies(staging_query)
    assert len(deps) == 1
    
    expected_spec = "test_namespace.chronon_signal_partitions/ds={{ ds }}/table_name=test_namespace.test_deps_staging_query"
    assert deps[0] == expected_spec
    
    deps_with_lag = get_staging_query_dependencies(staging_query, lag=2)
    assert len(deps_with_lag) == 1
    assert deps_with_lag[0] == expected_spec


def test_group_by_with_staging_query_dependencies():
    """Test GroupBy integration with explicit staging query dependencies."""
    staging_query = api.StagingQuery(
        metaData=api.MetaData(
            name="test_gb_staging_query",
            outputNamespace="test_namespace"
        ),
        query=api.Query(
            selects={"user_id": "user_id", "metric": "some_metric"},
            startPartition="2023-01-01",
            endPartition="2023-12-31"
        ),
        createView=True
    )
    
    # Create source
    source = StagingQueryEventSource(
        staging_query=staging_query,
        query=api.Query(
            selects={"user_id": "user_id", "metric": "metric"},
            timeColumn="ts"  # Required for GroupBy validation
        ),
        is_cumulative=False
    )
    
    # Get explicit dependencies
    staging_query_deps = get_staging_query_dependencies(staging_query)
    
    # Create GroupBy with explicit dependencies
    group_by = GroupBy(
        sources=[source],
        keys=["user_id"],
        dependencies=staging_query_deps,
        aggregations=[
            Aggregation(
                input_column="metric",
                operation=Operation.SUM,
            )
        ],
    )
    
    # Verify dependencies contain our staging query dependencies
    # The GroupBy may have processed the dependencies, so we check if ours are included
    assert len(group_by.metaData.dependencies) > 0
    
    # Check if the staging query dependency is included (it may be wrapped)
    found_staging_query_dep = False
    for dep_str in group_by.metaData.dependencies:
        dep = json.loads(dep_str)
        if "chronon_signal_partitions" in dep.get("spec", ""):
            found_staging_query_dep = True
            break
    
    assert found_staging_query_dep, f"Expected staging query dependency not found in: {group_by.metaData.dependencies}"