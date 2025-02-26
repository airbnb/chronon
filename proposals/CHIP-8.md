# CHIP-8: Hourly GroupBy Batch Jobs
**Author: Andrew Lee (Stripe) | Last Modified: 2024-04-02**

This CHIP introduces changes to support running hourly batch jobs for Chronon `GroupBy`s, which will enable: 
- more frequent batch data uploads for streaming `GroupBy`s
- hourly batch features

**Current Status:** Design discussion. Stripe contributors are aligned on the design but have not started implementation yet.

## Motivation
This change is motivated by serving latency demands and user requests for improved system capabilities.

### Improve Chronon Feature Serving Latency (on certain KVStore implementations)
Stripe uses Chronon in very high-volume and latency-sensitive applications. The proposed changes will further improve serving latency on top of existing changes like tile layering 
([PR](https://github.com/airbnb/chronon/pull/523), [PR](https://github.com/airbnb/chronon/pull/531)) and online IR caching ([CHIP](https://github.com/airbnb/chronon/blob/main/proposals/CHIP-1.md)). 

Stripe’s KVStore implementation consists of two separate underlying datastores. Batch data is stored in a write-once, read-many key-value store optimized for cheap bulk loads from S3. 
Streaming data is stored in a separate in-memory key-value store based off of Memcached that does not support range queries, meaning Stripe’s Fetcher must make numerous requests to the in-memory store to retrieve partial aggregates. 
As of today, after Stripe implemented tile layering and online IR caching internally, the request fanout factor to the in-memory store comprises a significant portion of the time spent fetching values for a Join. 
Running GroupByUpload jobs more frequently will dramatically reduce the request fanout, since fresher batch datasets mean fewer requests need to be made for streaming data. 


![image](https://github.com/airbnb/chronon/assets/80125871/e65f31b9-9db5-4753-b5a0-c327256a1945)
_With daily GroupByUpload jobs, Stripe's Fetcher makes 12 separate requests to the online store for streaming partial aggregates. Online store is depicted with Stripe’s tiling implementation with a 1-hour window resolution._

![image](https://github.com/airbnb/chronon/assets/80125871/74a11a56-64c9-48cc-bce9-a775a36ac549)
_If 4-hourly GroupByUpload jobs were enabled, Stripe's Fetcher would only make 4 separate requests to the online store for streaming partial aggregates._


### Hourly Batch Features
Batch features running at an every-few-hours cadence instead of a daily cadence will have fresher values- they’ll only be hours out of date instead of 1 day or more (depending on the pipeline driving the data source). 
While streaming accuracy features are significantly fresher than batch features at any cadence, hourly batch features are still very useful for some use cases e.g.:
- Users can write features on hourly data sources not available in a streaming context e.g. datasets that are enriched in batch, or datasets from 3rd party sources that are updated in bulk.
- In some cases, including at Stripe, the cost of online compute and storage (the in-memory online store) significantly exceeds that of batch compute and storage (the batch store with cheap bulk uploads).
Hourly batch features written on streaming sources allow ML engineers to strike a balance between freshness and infrastructure costs.

## New or Changed Public Interfaces
### New API configuration options
Users will configure their features with the following fields on Shepherd’s user-facing `Source`, `GroupBy`, and `Join` Python objects:
- `cadence` in `GroupBy` and `Join`, which determines at what cadence the GroupByUpload job runs. For `SNAPSHOT` accuracy `GroupBy`s, this determines how frequently the feature values will update and the minimum window length. 
Can be set to `ttypes.JobCadence.(HOURLY / FOUR_HOURLY / SIX_HOURLY / DAILY)`, defaults to `DAILY`.
- `sourceCadence` in `Source` which tells Chronon how often the source dataset is updated. Chronon will use this to establish dependencies on upstream sources in Airflow and when rendering queries on the underlying source dataset. 
Can be set to `ttypes.SourceCadence.(HOURLY / DAILY)`, defaults to `DAILY`. 
- `offline_schedule` in `Join` for determining the Airflow DAG that the scheduled job will run in. Can be set to `@hourly, @four_hourly, @six_hourly, @daily`.

Since we default to the daily behavior, these changes will be transparent to existing users with daily features.

```python
s1 = ttypes.Source(  
  events=ttypes.EventSource(
    table=...,
    query=Query(
      ...,
    ),
    # NEW
    sourceCadence=ttypes.SourceCadence.HOURLY,
  )
)

g1 = GroupBy(
  sources=g1,
  ...,
  accuracy=Accuracy.SNAPSHOT,
  online=True,
  # NEW
  cadence=ttypes.JobCadence.FOUR_HOURLY,
)

j1 = Join(
  left=...,
  right_parts=[
    JoinPart(group_by=g1),
    ...
  ],
  ...,
  # NEW OPTIONS
  offline_schedule="@daily",
  # NEW
  cadence=ttypes.JobCadence.DAILY,
)
```

### Invariants & Validations
We will enforce the following invariants to limit unintended behavior from misconfiguration. These will be checked at the Chronon Python level (i.e. before a Spark job is launched) to keep user iteration loops tight:
- `GroupBy`s must have a `JobCadence` longer than the `SourceCadence` of the Source (i.e. we disallow `N_HOURLY` cadences on a daily-produced source)
- `Join`s must have a `JobCadence` longer than the `JobCadence` of all `GroupBy`s
- On `SNAPSHOT` accuracy `GroupBy`s, window lengths must be at least as long as the specified `JobCadence` (doesn’t make sense to have a 1-hr windowed count that updates every 6 hrs). 
This is already enforced for daily `SNAPSHOT` accuracy `GroupBy`s ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/api/py/ai/chronon/group_by.py#L301))

## Proposed Changes
### Internal changes in Shepherd Batch Jobs
#### Decoupling source table & output `PartitionSpec`s in `Join` and GroupByUpload Jobs, and threading in user-facing options
Currently `Join` and GroupByUpload jobs will use one `PartitionSpec` when constructing queries on source datasets ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/spark/src/main/scala/ai/chronon/spark/GroupBy.scala#L554-L577)), 
shifting date/timestamps within job logic ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/spark/src/main/scala/ai/chronon/spark/Extensions.scala#L262-L266)), and producing partitioned outputs 
(GroupByUpload datasets, JoinPart tables, finalized `Join` tables). Since underlying data sources may be partitioned daily or hourly, and a `Join`’s `GroupBy`s may have different `JobCadence`s, 
the job logic will need to use a different `PartitionSpec` per Source and per `GroupBy` / `Join`. We will also propagate the above `GroupBy` changes to GroupByServingInfo ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/spark/src/main/scala/ai/chronon/spark/GroupByUpload.scala#L230) & [code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/spark/src/main/scala/ai/chronon/spark/GroupByUpload.scala#L230)), 
which contains metadata used by the online part of the system to make requests to `GroupBy`s online. 

This also involves creating new batch `Resolution`s ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/aggregator/src/main/scala/ai/chronon/aggregator/windowing/Resolution.scala#L25)) for the new `JobCadence` values we support, 
then changing the Job logic to specify `PartitionSpec`s and `Resolution`s correctly for given `Source`s and `GroupBy`s.
##### Correctness Tests
Write synthetic data sources w/ different partitioning schemes & `GroupBy`s. We’ll test:
- `Source` queries are correctly rendered given the source’s partitioning scheme
- GroupByUpload jobs produce outputs at the correct cadence (i.e. `2024010100, 2024010104` for `JobCadence.FOUR_HOURLY`)
- Deserialized GroupByUpload results are correct (i.e. `2024010104` contains values for the `GroupBy` as-of `2024-01-01 03:59:59.999 UTC`)

#### Joins with GroupBys with different `JobCadence`s
The current `Join` job will do a simple left join between intermediate JoinPart tables and the `Join`'s LHS table on the following keys: `(key, ts, ts_ds)` for `TEMPORAL` accuracy `GroupBy`s, and `(key, ts_ds)` for `SNAPSHOT` accuracy ones, 
where `ts_ds` is an internal column containing the string datestamp in which the timestamp of the row falls. Daily `SNAPSHOT` accuracy `GroupBy`s will produce one row per day, containing aggregations values at the end of a given day. 
But now a daily-cadence `Join` job may need to join intermediate JoinPart values for `SNAPSHOT` `GroupBy`s with values updating at daily and 4-hourly `JobCadence`s. We will accomplish this by constructing multiple internal columns on the LHS for each JoinPart `GroupBy` cadence present, 
then join each JoinPart only on the `ts_ds_X` internal column matching its `GroupBy`’s cadence, using the existing `coalescedJoin` method ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/spark/src/main/scala/ai/chronon/spark/JoinUtils.scala#L131)). 
`TEMPORAL` accuracy `GroupBy`s will also be joined on the internal column matching their cadences, in addition to `ts` and `key`, using the same logic.

![image](https://github.com/airbnb/chronon/assets/80125871/c5fe85c6-f7e8-471a-9f69-2ea79dc60e53)
_Left: Current final join between `Join` LHS and daily `SNAPSHOT` JoinPart tables, one `ts_ds` column._

_Right: Proposed final join between `Join` LHS and multiple `SNAPSHOT` JoinPart tables of different `JobCadence`s, with a `ts_ds_X` column for each._

#### Correctness Tests
- `Join` jobs produce outputs at the correct cadence
- `Join` job results are point-in-time correct for a mix of `GroupBy`s with different cadences and accuracies

### Changes to Fetcher
#### Propagating changes from GroupByServingInfo to Fetcher logic
In the previous section we will make `GroupBy`’s `JobCadence` and partitioning scheme available in GroupByServingInfo. Here we will ensure that logic throughout Chronon's Fetcher respects the newly-introduced fields. 
For example, Fetcher reads a batch timestamp attached to GroupByServingInfo and uses it to:
- Filter outdated responses from the streaming store prior to aggregation ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/online/src/main/scala/ai/chronon/online/FetcherBase.scala#L82))
- Construct the request to the KVStore to only fetch tiles after the latest batch data ([code](https://github.com/airbnb/chronon/blob/92a78f1c2607fd08bc4e3c076900db3f7b2d993b/online/src/main/scala/ai/chronon/online/FetcherBase.scala#L244))
## Migration Plan and Compatibility
As discussed in New or Changed Public Interfaces, the configuration options for hourly features are additive and will not impact existing features.

In addition to the correctness tests described in each section under Proposed Changes, Stripe will rigorously test this change internally in its QA environment and do a methodical rollout to partner teams’ features before contributing the change to the upstream.

## Rejected Alternatives
- Use one `PartitionSpec` for the smallest cadence in `Join` jobs and coerce other `Source`s/`GroupBy`s to match: Concretely, this means having the `Join` run hourly and treat any daily sources as hourly sources.
As part of this, we would use a capability added within Stripe’s Chronon fork called “Partition Column Overrides” and have users coerce the datestamp of their source partition into the hourly format expected 
(e.g. `yyyyMMdd` -> `yyyyMMddHH` would be accomplished by `day="concat_ws('', day, '23')"` in the `Source`'s `select`). Under the hood, Stripe’s Chronon fork injects that into the source query).
  - However, with this approach Spark can no longer use partition filtering and has to instead read the entire source dataset, which causes job runtimes to increase especially on large sources 
(e.g. the entirety of an O(charges) dataset instead of the partitions required by the longest window length)
  - Additionally, users would effectively need to treat the `Join` as hourly (CLI args for specifying date ranges, hourly-partitioned outputs) even if it had mixed daily & sub-daily `GroupBy`s, which would break our existing users’ pipelines and is frankly unintuitive.
- Intermediate JoinPart `Join` implementation: Rather than attempting to make a complex join condition that tries to match the LHS timestamp column to the nearest datestring of a JoinPart cadence, 
we prefer the approach of creating extra columns. This allows us to reuse `coalescedJoin` as-is, specifying the appropriate column as a join key.
