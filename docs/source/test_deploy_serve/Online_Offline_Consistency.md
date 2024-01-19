# Online Offline Consistency

## Problem
Chronon produces data in two contexts. **Offline** as hive tables and 
**Online** via a low latency kv-store. In the context of a machine learning
application, training uses offline data and inference uses online data. To
achieve best model performance, offline data and online data need to 
be consistent with each other. Or there would be a difference in model 
performance while training vs. inference, usually in a bad way. 
This framework allows users to measure inconsistency automatically.


## Causes of inconsistencies

While 100% consistency is desirable, it is not always possible.

Realtime features consume streams of data(kafka topics) that have inherent 
network latencies. For kafka, in at-least-once mode producers need to write 
to multiple disks before the message can be sent to the consumers. Finally, 
there is additional latency to write kv-store before the model can fetch the 
update feature for inference. This is usually in the order of ms to seconds.

Computing and uploading non-realtime features to kv-store 
takes time. For example, midnight accurate features, need to wait for their 
upstream data to arrive and then need to be transformed into features and 
uploaded into kv-store. The total time to refresh batch features could be in
the order of minutes or hours. This inconsistency is usually centered around 
midnight until the bulk upload of new features to kv store finishes - for 
features that need to refreshed daily.     

## Design

We log inference queries, the chronon's responses along with their timestamps.

```sql
CREATE TABLE IF NOT EXISTS <namespace>.query_logs (
    key_bytes STRING COMMENT 'Avro encoded key bytes', 
    value_bytes STRING COMMENT 'Avro encoded value bytes', 
    ts_millis BIGINT COMMENT 'Timestamp of the fetch', 
    query_source STRING COMMENT 'Name of environment or service calling fetch'
)
PARTITIONED BY (
    ds STRING,
    join_name STRING
)
TBLPROPERTIES (
    'retention' = '90'
)
```

We use the joins uploaded from metadata and schemas in batch uploads to 
construct the avro schema of join keys and values. This step happens in the
spark driver.

We want to apply sampling in such a way that we capture all instances of 
a subset of keys, instead of randomly selecting a subset of all queries.
This is to reduce amount of keys that the join's bloom filter accepts while
scanning source data.

Filtering in fetcher using: 
  `hash(key_bytes) % 100 * 1000 <= sample_percent * 1000`

### Spark job logic
  1. Filter on 
     - `join_name = '<join_name>' and `
     - `ds between earliest_unavailable_ds AND run_ds`  
  2. Expand key_bytes & value bytes into separate columns and save as table
     - `<namespace>.<join_name>_logged`
  3. Fetch join conf from kv store
  4. Replace left source with logged table. (prefix value_columns with logged)
  5. Compute offline join. (earliest unavailable ds till now)
     - Save to `<namespace>.<join_name>_comparison`.  
  6. Consistency Metrics to compute 
     1. equality comparison: (universal to all types) 
        - count(a != b) / count(*), (mismatch)
        - count(a = null & b != null) / count(*) (missing) 
        - count(a != null & b != null) / count(*) (extra) 
     2. numeric comparison: (specific to numeric types, everything will be cast to doubles)
        - sum(abs(a - b))/sum(a+b) (smape) 
        - b - a  (delta distribution)
        - a (logged distribution) 
        - b (backfilled distribution)
     3. sequence comparison:
        - length(a) (logged_length distribution)
        - length(b) (backfilled_length distribution)
        - edit_dist(a, b) (Levenshtein distance distribution)
           - inserts and deletes are counted separately to distinguish extra vs missing
           - replacements are not allowed
     4. map comparison: [TODO]
        - missing_keys,
        - extra_keys, 
        - mismatched_keys, 
        - left_keys, 
        - right_keys
        - values are also compared according to 1, 2, 3
  7. Consistency metrics are produced as a hive table per join
        - `<namespace>.<join_name>_consistency_metrics`
        - partition columns: ds, hr_minute, group_by_name, 
        - metadata columns:  feature_name, metric_name
        - metric columns:    histogram, number (only one of histogram or number is present)
  
 

## Choosing the right quantile sketch

"Compressing" a series of values into a histogram is what we want - 
to "summarize" inconsistencies across various data points. There are several available
sketching algorithms in java. These algorithms broadly falls into two categories,
"practical" & "theoretical". Where theoretical ones have a proven lower bound on errors
and practical ones don't. Leading both the packs are t-digest and ReqSketch. We will only 
be considering these two sketches.

[Summarily](https://arxiv.org/pdf/2102.09299.pdf), ReqSketch is accurate at all ranges compared to t-digest. 
ReqSketch takes slightly more space(2.75 kb vs 2.65 kb) - but is much faster(>2x) to update, serialize and deserialize.
Since we are going to ever hold one sketch per feature name per executor, space is less of a factor
than accuracy during aggregation than the speed of update.


## Choosing the right aggregation strategy

Merge-combine shuffles deltas across all machines in the combine stage to all machines in the merge stage.
Tree aggregates rolls up deltas (imagine a binary tree). Tree aggregate, mandates that all data eventually
fit in a single machine. Merge-combine is more suited to situations where output data is still large.

Consistency metric outputs are decidedly - "small data". 
Pessimistically assuming that each join has 1000 features and feature has 10 metrics that are heavy(distribution),
We will have at most 30MB of consistency summary data (as deltas) per machine. 


## Estimating size of consistency summary 
Once we have the final merged sketch for distribution, we will "bin" the quantiles 
into float values. So for min, p5, p25, p50, p75, p95, max - 28 bytes. 

Currently, will have a maximum of 4 distributions, 3 Longs and 1 double per feature. About 400 Bytes.
For a join with 1000 features, this comes to 118 KB per day of data. For 100 features, it is 12 KB.
For a three-month range, it will be 9 MB and 0.9 MB respectively.

Which is to say that displaying 10,000 metrics at one time is the current
breaking point of the design. 

## Future work

### Specify groupBy lag
Chronon could allow users to specify a write-lag as milliseconds. 
Offline, joining with observations/query log would shift the query timestamps 
ahead by the specified amount. One could use this consistency-measurement 
framework to estimate the time interval to shift. Since this would vary by 
data source, this needs to be set at group-by level. Once set, this would 
make any join that uses features from the group-by more consistent.

### Visualization
- Upload to kv store - key is join, value is metrics summary json, ts is unix_time(ds) * 1000 
  - charts needs to work from local laptop, where submitting spark job is hard / not possible.
- Download from kv store, and serve using cask + scalatags + uplot.js
  - cask + scalatags backend [example](https://www.lihaoyi.com/post/SimpleWebandApiServerswithScala.html)
  - [uplot.js charting](https://github.com/leeoniya/uPlot)
  
### Standalone groupBy consistency
- Sometimes, people directly use a groupBy without using a join. 
  But, consistency and logging is still desired for the groupBy.
- We add one more column `type` which is either `groupBy` or `join`.

### Payload request
- Join request, and group by request sometimes consists of additional columns
  - Like session_id and search_id etc, that are not join keys.
  - These columns are required by users who consume the `_logged` tables directly.
  - This is not related to consistency but more related to logging.  
  

