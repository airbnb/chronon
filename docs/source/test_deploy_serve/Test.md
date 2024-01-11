# Testing `GroupBy`s and `Join`s

This doc assumes that you have already a `GroupBy`, `Join`, or `StagingQuery` created that you wish to test.

Testing will be for one of the following flows:

1. [Analyze](#analyze) - A quick job that sanity checks your `GroupBy` or `Join` by returning some metadata such as the output schema, row counts, etc. This is a lightweight validation that can optionally be used before running a more compute intensive backfill.
2. [Backfill](#backfill) - Computes historical feature values and writes them to the output table for inspection.
3. [Serve](#serve) - A multi-part workflow that includes uploading data to the online KV store, in batch and streaming (if applicable), and fetching feature values for that entity.

The first step in any of these workflows is to [Compile](#compile) the Chronon entity which you wish to test.

## Compile

Compiling converts the python Chronon definition into thrift - as json - which can be passed to the Chronon engine.

From the root of the main Chronon directory, run:

```bash
compile.py --conf=path/to/your/chronon_file.py
```

The path that you pass should either commence with `group_by/`, `join/`, or `staging_query/`

This will produce one 

## Analyze

The analyzer will compute the following information by simply taking a Chronon config path.
* Heavy hitters of all the key columns - for users to filter out keys that are not genuine, but skewed.
* A simple count of items by year - to sanity check the timestamps.
* A row count - to give users a sense of how large the data is.
* Output schemas - to quickly validate the sql statements and understand the output schema.
* Validations for JOIN config - to make sure the join conf is valid for backfill. Here is a list of items we validate:
  * Confirm Join keys are matching on the left and right side
  * Confirm you have access to all the tables involved in the join
  * Confirm the underlying tables have partitions going as far back as needed for backfill aggregation
  * Provide a list of GroupBys which have `startPartition` filters for sanity check. GroupBy `startPartition` should not be after Join backfill start partition.

Please note that these validations will also be executed as a prerequisite check for join backfill. In the event of any validation failure, the job backfill will be aborted to ensure better efficiency.


### How to run analyzer

```
# run the analyzer
run.py --mode=analyze --conf=production/joins/<path_to_conf_file> --enable-hitter
```

Optional parameters:

`--endable-hitter`: enable skewed data analysis - include the heavy hitter analysis in output, only output schema if not specified

`--start-date` : Finds heavy hitters & time-distributions for a specified start date. Default 3 days prior to "today"

`--count` : Finds the specified number of heavy hitters approximately. The larger this number is the more accurate the analysis will be. Default 128

`--sample` : Sampling ratio - what fraction of rows into incorporate into the heavy hitter estimate. Default 0.1

## Backfill

You can run the compiled configs (either `Join`/`GroupBy`/`StagingQuery`) with `backfill` mode to generate data.

```sh
run.py --mode=backfill --conf=production/joins/team/join.v1 --ds=2022-07-02
```

This runs a spark job which will compute the data.

Most of the time, backfilling a `Join` is what you want because this backfills any `GroupBy`s used in the join as well.

If you want to produce a snapshot accurate table for the aggregations in a group by you can add `backfill_start_date`. Backfilling `GroupBy` is usually an analytics use-case, as online uploads have their own flow (see [Serve](#serve) below).

## Serve

You can either serve a `GroupBy` on its own, or a `Join` if you wish to fetch results for many `GroupBy`s together in one request.

Manually running the test workflow for serving is optional. If you've validated that your Chronon config generates the correct results in backfill runs, then most of the time you can simply merge your config and let the scheduled airflow runs orchestrate the necessary steps to enable serving.

## GroupBy Upload

You need to upload some data into a KV store to be able to fetch your data. For a join, this means:

1. All the relevant `GroupBy`'s data should be uploaded to the KV store.
2. The `Join`'s metadata should be uploaded to the KV store (this allows Chronon to know which `GroupBy`s to fetch when the request comes in).

For a `GroupBy`, you just need to run one upload.

First, make sure your `Join`s and `GroupBy`s as marked as `online=True`. This is an argument you can pass to both objects like so:

```python
your_join = Join(
    ...
    online=True,
)

# or

your_group_by = GroupBy(
    ...
    online=True,
)
```

Once you have marked a particular Chronon definition as online and compiled it, you need to upload the relevant `GroupBy`'s data into your KV store. 

The following command will generate a table with key-value bytes that's ready for upload to your KV store:

```bash
run.py --mode upload --conf production/group_bys/your_group_by.v1 --ds 2023-12-01
```

and similarly for Join

```bash
run.py --mode metadata-upload --conf production/joins/your_join.v1 --ds 2023-12-01
```

The next step is to move the data from these tables into your KV store. For this, you need to use your internal implementation of KV store integration. This should already be what your Airflow jobs are configured to run, so you can always rely on that, or if your Chronon team has provided you with a manual upload command to run you can use that.

## Fetching results

You can fetch your uploaded conf by its name and with a json of keys. Json types needs to match the key types in hive. So a string should be quoted, an int/long shouldn't be quoted. Note that the json key cannot contain spaces - or it should be properly quoted. Similarly, when a join has multiple keys, there should be no space around the comma. For example, `-k '{"user_id":123,"merchant_id":456}'`. The fetch would return partial results if only some keys are provided. It would also output an error message indicating the missing keys, which is useful in case of typos.

```bash
python3 ~/.local/bin/run.py --mode=fetch -k '{"user_or_visitor":"u_106386039"}' -n team/join.v1 -t join
```

Note that this is simply the test workflow for fetching. For production serving, see the [Serving documentation](./Serve.md).

## Testing streaming

You can test if your conf is a valid streaming conf by specifying a `--mode=local-streaming`.

```sh
run.py --mode=local-streaming --conf=production/group_bys/team/your_group_by.v1 -k kafka_broker_address:port_number
```

The `-k` argument specifies a kafka broker to be able to connect to kafka. You need to know which kafka cluster your `topic` lives in.

## Online offline consistency metrics computation

After enabling online serving, you may be interested in the online offline consistency metrics for the job. Computation of these can be turned on by setting the `sample_percent` and `check_consistency` in the join config like so:

```python
my_join = Join(
    ...
    sample_percent=0.1, # Samples 10%
    check_consistency=True
)
```

The `sample_percent` param will enable logging during fetching phase, and the `check_consistency` param will enable the online offline consistency check DAG to trigger a spark job to compare the logged events with offline join job after data landing in the warehouse. The DAG will be named: `online_offline_comparison_<team_name>_<join_name>`.

The spark job will write the result to a flat Hive table in the pattern of `<output_namespace>.<team_name>_<join_name>_consistency`.

See more details of what it computes [here](https://sourcegraph.d.musta.ch/github.com/airbnb/chronon/-/blob/docs/source/Online_Offline_Consistency.md).


## Useful tips to work with Chronon

### Getting the argument list

Most of the above commands run a scala process under the python shim, which could take a lot of arguments. You can see the help of the scala process by using `--sub-help`.

```bash
[gateway_machine] python3 ~/.local/bin/run.py --mode=fetch --sub-help
```

This example will print out the args that the fetch mode will take. You can do the same for other modes as well.

```bash
Running command: java -cp /tmp/spark_uber.jar ai.chronon.spark.Driver fetch --help
  -k, --key-json  <arg>        json of the keys to fetch
  -n, --name  <arg>            name of the join/group-by to fetch
      --online-class  <arg>    Fully qualified Online.Api based class. We expect
                               the jar to be on the class path
  -o, --online-jar  <arg>      Path to the jar contain the implementation of
                               Online.Api class
  -t, --type  <arg>            the type of conf to fetch Choices: join, group-by
  -Zkey=value [key=value]...
  -h, --help                   Show help message
```
