# Staging Query

A StagingQuery can be used to express free form ETL (including SQL joins/group by) within Chronon. They are typically used
to express more complex logic that won't fit into the simple `query` facility provided by the source. However this logic can't be used to produce features that are updated in realtime. They can only be refreshed in batch mode.

One common use case for staging query is to prepare the `left` side of the join (see the [Join](./Join.md) documentation if you're unsure what this is). Because the left side needs to provide all of the primary keys used across all of the `GroupBy`s within the `Join`, sometimes you need to join a few tables together to get all the keys and the relevants timestamps side by side.

For example, let's say we were using a theoretical `fct_purchases` table as the left side of our join, however, we need a couple additional fields such as `user_type`, `user_country` and `email` that we can get by joining to a `dim_users` table.

In this case, the `StagingQuery` might look like this:

```python
v1 = StagingQuery(
    query="""
        SELECT
            a.id_user,
            a.ts,
            a.ds,
            b.user_type,
            b.user_country,
            EMAIL_PARSE(b.email) as parsed_email,
        FROM 
            data.fct_purchases a 
        JOIN
            data.dim_users b 
        ON
            a.id_user=b.id
        AND
            a.ds = b.ds
        WHERE
            a.ds between '{{ start_date }}' AND '{{ end_date }}'
        AND 
            b.ds between '{{ start_date }}' AND '{{ end_date }}'
    """,
    startPartition="2020-04-01",
    setups=[
        "ADD JAR s3://path/to/your/jar",
        "CREATE TEMPORARY FUNCTION EMAIL_PARSE AS 'com.you_company.udf.your_team.YourUdfClass'",
    ],
    metaData=MetaData(
        dependencies=["data.users", "data.purchases"], # Used by airflow to setup partition sensors
    )
)
```

And then you could use it in your `Join` as follows:

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

Note: The output namespace of the staging query is dependent on the metaData value for output_namespace. By default, the 
metadata is extracted from [teams.json](https://github.com/airbnb/chronon/blob/main/api/py/test/sample/teams.json) (or default team if one is not set).

**[See more configuration examples here](https://github.com/airbnb/chronon/blob/main/api/py/test/sample/staging_queries)**

## Date Logic and Template Parameters

**IMPORTANT: Running a `StagingQuery` for a particular day (`ds`) does not only produce data for that `ds`, but rather everything from the `Earliest Missing Partition` up to the `ds`, where the `Earliest Missing Partition` is defined as the `earliest existing partition + 1` if any partitions exist, else the `startPartition` defined on the `StagingQuery`.**

Usually this results in one big job for the first backfill, then smaller jobs for incremental frontfill.

This method of backfilling data is often faster for long ranges, however if you have concerns about any given run being too large to complete, you can also use the `step-days` argument to configure a maximum number of days to include in any given run:

```python
v1 = StagingQuery(
    ...
    customJson=json.dumps({
            "additional_args": ["--step-days=30"], # Sets the maximum days to run in one job to 30
        }),
)
```

This argument can also be passed in at the `CLI` when manually running your `StagingQuery`.

Because of this, you often want to write your `StagingQuery` using various templated parameters for dates, for example `'{{ start_date }}'` and `'{{ end_date }}'`, as you can see in the example above. The parameters can be easily extended. The current full list of parameters are:

- `'{{ start_date }}'`: the `Earliest Missing Partition`, as defined above.
- `'{{ end_date }}'`: The end ds of the chunk that is currently being run. Chunking is determined based on the entire range that is requested for computation, broken into pieces of maximum size `step-days` (see documentation on `step-days` above). If `step-days` is equal to or greater than the number of days in the entire requested range, then `end_date` is the same as `latest_date`. `end_date` can never exceed `latest_date`.
- `'{{ latest_date }}'`: the ds for which computation was requested. If running from the CLI, it defaults to the day before yesterday UTC. If the job gets broken up into smaller chunks due the to `step_days` argument, `latest_date` will be the same in all the runs, and will equal the `end_date` of the last chunk.
- `'{{ max_date }}'`: the latest existing partition in the table, does not depend on input parameters to the job, but only on available partitions in the source table. It requires the source table as an argument, for example: `'{{ max_date(table=data.some_table) }}'`

## `StagingQuery` in Production

Once merged into production, your `StagingQuery` will get scheduled for daily run within your `{team}_staging_queries` DAG in airflow. The compute task will get an upstream `PartitionSensor` for each of your table dependencies, which will wait for that day's partition to land before kicking off the compute. See above for an example of how to set dependencies.
