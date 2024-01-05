# StagingQuery

A StagingQuery can be used to express free form ETL (including SQL joins/group by) within Chronon. They are typically used
to express more complex logic that won't fit into the simple `query` facility provided by the source. However this logic can't be used to produce features that are updated in realtime. They can only be refreshed in batch mode.

```python
v1 = StagingQuery(
    query="select * from namespace.table a JOIN namespace.table_2 b on a.key=b.key ",
    startPartition="2020-04-01",
    setups=[
        "ADD JAR s3://path/to/your/jar",
        "CREATE TEMPORARY FUNCTION YOUR_FUNCTION AS 'com.you_company.udf.your_team.YourUdfClass'",
    ],
    metaData=MetaData(
        dependencies=["namespace.table", "namespace.table_2"],
    )
)
```

The StagingQuery can then be used in both GroupBy and Join. For example:
```python
from staging_queries.team_name_folder import file_name
from ai.chronon.utils import get_staging_query_output_table_name
v1 = Join(
    left=EventSource(
        table=get_staging_query_output_table_name(file_name.staging_query_var_name)
        ...
    )
)
```

Note: The output namespace of the StagingQuery is dependent on the metaData value for output_namespace. By default, the 
metadata is extracted from [teams.json](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/teams.json) (or default team if one is not set).

**[Click here for more configuration examples](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/staging_queries)**
