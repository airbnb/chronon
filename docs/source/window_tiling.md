# Window Tiling Algorithm

## Context

In zipline, we do what's called a Point-In-Time-Correct(PITC) backfill - which is best illustrated with an example.

We get a table containing timestamped rows. For example,

| user_id |      timestamp      |
|---------|---------------------|
|  alice  |    2021-09-30 5:24  |
|   bob   |    2021-10-15 9:18  |
|  carl   |    2021-11-21 7:44  |

We are then required to enrich this table with data from various data sources aggregated using operands like sum/count etc, by varying window sizes.

In the above example, let's say the user wants us to enrich the table with two new columns representing

1. User's count of views of listings in the last 5 hours - from a view event source.
2. User's average rating of listings in the last 90 days - from a database's rating table.

The user would expect an output table that looks like

| user_id |      timestamp      | views_count_7d  | avg_rating_90d |
|---------|---------------------|-----------------|----------------|
|  alice  |    2021-09-30 5:24  |       10        |       3.7      |  
|   bob   |    2021-10-15 9:18  |        7        |       4.5      |
|  carl   |    2021-11-21 7:44  |       35        |       2.1      |

Users use this data, typically, as features, to train machine learning models. These models also require these aggregations while serving to make predictions.
So online we get a request that translates to something like - "produce the view count in the last 5hrs, and average rating in the last 90 days, **ending now** for a user named Bob".

**In summary** - Zipline does three main things

1. Enrich a hive table with aggregates, that are PITC.
1. Maintain an online view of these aggregates - that is updated in real-time and optimized for low-latency reads.
1. Ensures that generated data is consistent between the hive table and the online serving environment - by measuring the difference between online serving logs and offline generated data.

### Alternatives

1. **Log-and-wait**: We don't really need to backfill, if we can just build the serving system and log the aggregates. The viability of this approach depends on how long we need to wait to accumulate enough data to re-train the models. If one needs months worth of data to train a model, backfilling becomes necessary.
1. Hand build the backfills: This is prone to becoming inconsistent with the serving system. As we will see in the remainder of this document, this is a non-trivial problem to make performant.

### Data requirements

In this example, there are two kinds of sources - with certain assumptions.

1. View Event Source - is an **Event** source. Zipline requires that there is:
    1. A corresponding offline log - as a hive table partitioned by date - each partition containing only events that occurred during the partition's date.
    1. A stream of view events in a message bus
1. Rating Source - is what is called an **Entity** source. This is a database table somewhere that receives inserts, updates, and deletes to rating rows. Zipline requires that there is -
    1. A hive table containing a snapshot of the Ratings table - taken at midnights - partitioned by date.
    1. A hive table containing logged mutations - partitioned by date - each partition containing mutations that occurred on that date.
    1. A stream of rating table changes/mutations (2 & 3 constitute a CDC system)

### Note on Configurability

1. Users can choose to forego PITC and settle for midnight accurate aggregations. In which case the streams and the mutation tables are not necessary. Also when streams or mutations are not present, you are forced to settle for non-PITC.
1. Users can choose to disable online serving, in which case the streams are not necessary.
1. Users can also choose to disable offline data generation - but all the mentioned sources are still necessary to bootstrap and batch correct the serving system.
1. Users can choose to use pre-aggregated data - in which case real-time aggregation doesn't make any sense - so the aggregations will be refreshed only at fixed time intervals. Intervals are determined by the landing cadence of pre-aggregated data.

### Note on Terminology

1. Event sources are also referred to as fact sources or measure sources in the offline world.
1. Similarly Entity source is sometimes referred to as a dimension source.
1. Online serving of these aggregates and joins is very similar to materialized views in a database. But the main difference is, that Zipline pushes most of the computations to the write side of materialized view, traditional DB views are computed on write.

---

## Tiling Algorithm

In the rest of this document, we are going to describe the algorithm that powers the efficient generation of PITC data for offline usage. To motivate the approach we are going to simplify the problem statement significantly - while still retaining its important characteristics.

We will start with a brute-force approach, with data that could fit in a single machine and evolve it into an optimized distributed algorithm.

Here is the simplified problem statement.
Given:

1. A table of `queries` : (`user`, `timestamp`) and
1. A table of `views` (`user`, `timestamp`, `view_id`)

We need to produce

1. A table with schema (`user`, `query_timestamp`, `view_count_7d`). One row per row in the queries table.

In other words, we are enriching the query table with the last 7 day views. Timestamps are in milliseconds everywhere.

## Naive approach

Expressed in sql the query the naive solution would look like

```sql
SELECT user, query.timestamp as query_timestamp, COUNT(view_id) as view_count_7d
FROM queries JOIN views ON
  queries.user = views.user AND
  view.timestamp < queries.timestamp AND
  view.timestamp >= (queries.timestamp - 7d) -- 7 * 24 * 3600 * 1000 milliseconds
GROUP BY user, query_timestamp     
```

It is not very hard to verify that this will produce the expected result. But we need to follow this with two very natural questions. Please take a minute to answer them for yourself.

1. What is the time complexity of this query? For the sake of simplicity - assume this is happening on a single machine and there are n queries and n views evenly distributed across k keys. You can see the answer [here](#naive-window-tiling-complexity).
1. How can we make it faster?

## Daily rollups

If you are reading this and have looked at the answer to 1. You might be wondering if pre-aggregating the view counts by days might help.
Let's say we have another table now, with the pre-aggregated view counts by days.
`view_dailies`: `user`, `view_count`, `ds`

Unfortunately even for very small amounts of data with a handful of skewed keys, this approach will still result in a quadratic explosion. We would indeed limit the quadratic explosion within a day. So it won't turn millions of rows into trillions of operations, but instead into billions of operations (~ trillion / number of days to generate data ^ 2). Which is still a problem. Especially since the query plan still puts all these rows for a given key into the same executor.

## Reframing as arrays

We will reframe the problem as arrays, and remove the idea of keys just to focus on the explosion problem. Hopefully, the perspective change will result in a better strategy.

Given two arrays structs:

1. An array `queries` of query timestamps
1. An array of `views` and view timestamps

Produce

1. An array of tuples: (`query_timestamp`, `view_count_5h`)

And the naive approach expressed as python/pseudo-code - is still quadratic.

```python
result = []
for query_ts in queries:
    view_count = 0
    for view_ts in views:
        if view_ts < query_ts and view_ts > query_ts - millis_7d:
            view_count += 1
    result.append((query_ts, view_ts))
# result now contains the desired data
```

Take a minute to think about how to optimize this.
Hint: sorting.
A possible optimization strategy is explained below the code listing. Please spend a minute thinking about how you would do it before proceeding.

```python
result = []
start = 0
end = 0
count = 1
sorted_views = sorted(views)
for query_ts in sorted(queries):
    query_start = query_ts - 7 * day_millis
    # scan forward the start cursor and decrement the counter
    while start < len(sorted_views) and sorted_views[start] < query_start:
        start += 1 
        count -= 1
    # scan forward the end cursor and increment the counter
    while end < len(sorted_views) and sorted_views[end] < query_ts:
        end += 1
        count += 1 
    result.append((query_ts, count))
# result now contains desired data.
```

The idea now is to sort both the view and queries - and scan the arrays linearly. A bit like merge sort, but with two cursors on views to maintain the window count.

The complexity of this approach is dominated by sorting - nlogn. But the difference from the prior approach is quite significant (nlogn vs n^2 at 100k is )
The scans are linear. We can do a constant number of operations per cursor move because we are leveraging an important property of summing - it is "subtractable". This is not the case with a lot of aggregations - simple examples are 'min'/'max'/'first'/'last'.

Another problem is that, for a good majority of ML cases, the queries are much smaller than the views. Queries are essentially limited by the number of available labels. Backfilling training feature data is essentially pointless if you don't have a corresponding label to train with.

Another inherent assumption that is problematic is that we can scan the views and hold them in memory. Usually, this means that we are trying to load the entire views source into memory for the specified window size. As discussed earlier, queries are much smaller (at least two orders of magnitude) than views. Loading the entire views source in memory is going to be extremely costly.

### Let's formalize the new constraints

1. Single-pass over Views source - so can't sort.
2. Support un-subtractable aggregations like max, min, sum, last, etc.,

We can still think about the problem in our simplified setting. By assuming that we have a queries iterator, and a views array.

The intuition behind the next approach is really simple. As we process a view event, we want to increment just the queries that it affects. Keeping the queries sorted makes it easy to search for queries to update when we process an event.

So we can binary search the sorted Queries array and begin updating until necessary.

```python
query_results = [(ts, 0) for ts in queries]
for view_ts in views:
    index = binary_search(view_ts - 7 * millis_in_day, queries)
    while index < len(queries) and queries[index][0] <  view_ts:
        query_results[index][2] += 1  # update operation
        index += 1
```

You might have already guessed that I am going to say - this is still problematic.
We are doing too many update operations. In zipline we allow for very large windows
and we essentially would still be performing a `queries * ratings` number of update operations. But this is a good starting point to do something better.

Ideally, we want to update to be better than `O(n)`. Can we do `O(log(n))` given a `start` index and an `end` index that are binary searched? Perhaps we can update a fixed set of `O(log(n))` sub-windows/window-tiles that can be post processed in `O(n)` into the required result.

## Interval tree approach

Imagine updating a tree structure - where leaf nodes represent the individual queries, but the parent nodes represent ranges of queries. During pro-processing we can trickle down the updates.

1. Given a sorted array of query timestamps, and an event - with a timestamp, we can binary search and find out which indices to update. Let's say we found out from binary searching that we need to update timestamps indexed between 4 & 13 as shown below.

![tiling input](../images/tiling_input.png)

1. Now we can overlay a complete binary tree on this array of query timestamps, and try to update a smaller set of range nodes.

![tiling tree](../images/tiling_tree.png)

Imagine we have a Data Structure to efficiently perform updates that has the following api:

```python
class IntervalTree:
    def __init__(elements: List[QueryTimestamp]):
    def update_nodes_in_range(start: int, end: int):
    def propagate_updates_to_leaf_nodes():  # collapses all aggrgegates from the tree
```

Note: I removed `self` from the class method above to improve readability.
we can then simply do

```python
def tree_tiling_sum(queries, event_iterator):
    query_tree = new IntervalTree(queries)
    for event_ts in event_iterator:
        start = binary_search(event_ts, queries)
        end = binary_search(event_ts + 7 * millis_in_day, queries) - 1
        query_tree.update_nodes_in_range(start, end)
    return query_tree.propagate_updates_to_leaf_nodes()
```

There are many ways to implement the tree. But since it is a complete binary tree (almost), we can store the tree nodes in an array. We can simply find the parent of the node by dividing - index by `⌊index/2⌋`, and the two children by `index*2` and `index*2 + 1`. (or by using shift operators). [This code listing below](#scala-interval-tree-code-listing) updates in O(logn) finalize, in O(n) extra memory for tree - where `n = # of queries`.

TODO: convert to python pseudo code listing from Interval tree implementation in zipline scala code.

### Note on row vs column orientation

Lets stop here and think about the generalized case for a minute. Where we have 10s-100s of feature aggregates being computed in the same pass. What is the best orientation for this computation - row or column? It is more efficient do the tree traversal only once - across the all features. As a result, row orientation is going to be more efficient.

### Note on distributed compute

We can perform what is called a Broadcast Hash Join

1. Send a copy of the queries to all worker nodes - (spark uses torrent protocol to reduce congestion and speed up the transfer)
1. Stream partitions of events through the interval tree structure using the pseudo code listed above.
1. Shuffle and Reduce the query_result from each of the nodes into a final query_result.

## Still not fast enough

The assumption that we can fit queries in memory is based on the fact that most use-cases don't tend to have enough labeled data to train with in the first place. There is a class of very impactful models that this is not true for - search ranking & recommendation systems. We recieve a label as soon as a user decides to click or spend time on the recommended item. For search use-cases, the labelled training data can be as large as all the searches on the platform.

So to really support ranking/recommendation scale we need to build for a case where both queries and views can't fit in memory. But instead of always piling on tough constraints, lets relax the window constraint and see if that can help.

## Windowing Semantics

### **Sliding Window**

What we have been using so far is a sliding window. The window range is determined by query - `[query_ts - window, query)`. So each query can potentially have a different result. This has the benefit of being able to incorporate the most recent information - which is essential for machine learning models. But the down-side is that it is harder to re-use computation when the result can vary with every query.

### **Hopping windows**

A 1 hour window that hops by 5 minutes - is simply - a window with 12 internal hops, that contain 12 partial aggregates, and this structure changes value only once every 5 minutes. The window range is determined by - `[round_nearest(query_ts - window, hop_size), round_nearest(query_ts, hop_size))`. This essentially means that all queries that fall into the 5 minute bucket, have the same result. But since it doesn't capture the most recent events, which hold very important signal for machine learning models.

### **Sawtooth Windows**

We introduce what's called a Sawtooth window in zipline to merge the **most-recent-information-capture** advantage of Sliding Windows and **re-usability of hop computation** of Hopping Windows. In Sawtooth windows - the tail is hopping, and the head is sliding. `[round_nearest(query_ts - window, hop_size), query_ts)`.
So for all queries within a 5 minute hop, the left boundary is the same! Only the right boundary changes. So we can now break the window into a head and a long-tail.

- head: The most recent data upto nearest 5 minute mark `[round_nearest(query_ts, 5m), query_ts]`
- tail: Everything else - `[round_nearest(query_ts - window, hop_size), round_nearest(query_ts, 5m))`

We can pre-compute the tail once per all the queries that are in a hop - five minute bucket (12:00 - 12:05).

We are still left with the task of computing the head of the sawtooth window. But computation of the tail is simple - we sort events and queries in the head and do a merging-scan. This is significantly more distributable without having to resort to any quadratic compute.

The actual implementation is extremely generalized - in the same pass we compute multiple aggregations of various types over multiple columns with multiple window sizes and multiple buckets as a distributed spark topology. It does further optimizations to how we compute. It can be very overwhelming to see that code.

The next section is going to be a lot more code than you saw so far. But hopefully it is broken down enough to not hinder understanding. In the end, there will be an image of a spark execution plan that can help you map the code visually.

Lets create some helper functions in python pseudo code first - some of which are very common in distributed compute systems.

```python
def add(a, b):  # works for lists and numbers alike
    return a + b

def reduce_by(elements, key_func, reduce_func = add):
    result = {}
    for element in elements:
        key = key_func(element)
        if key not in result:
            result[key] = element
        else: 
            result[key] = reduce_func(result[key], element)
    return result

# collect list of elements
def group_by(elements, key_func):
    return reduce_by([[elem] for elem in elements], key_func)

def map_values(map_, func):
    return {key: func(value) for key, value in map_.items()}

def left_equi_join(map1, map2):
    result = {}
    for key, value in map1.items():
        result[key] = (value, map2.get(key, None))
    return result

def nearest_multiple(number, factor):
    return math.floor(number/factor) * factor

def hop_5m(ts):
    return nearest_multiple(ts, 5 * 60 * 1000)
```

We need an important function to compute the heads of the windows. Given two lists of timestamps, a query_list and event_list, we need to create an output list, that contains `[(query_timestamp, event_count_until_query_timestamp)]`

```python
def cumulative_merging_sum(query_list, event_list):
    queries = sorted(query_list)
    events = sorted(event_list)
    result = []
    index = 0
    count = 0
    for query in queries:
        while events[index] < query:
            count += 1
            index += 1
        result.append((query, count))
    return result
```

We need another function to compute tails of the windows. Note that there are much better ways to compute the tails by re-using intermediate elements - if you recall the interval tree stuff.

```python
# queries_5m is list of unique 5m hop start timestamps of queries
# event_partials_5m is list of tuples, (5m hop start timestamp, partial_count_in_5m)
def compute_tails(queries_5m, event_partials_5m):
    start = 0
    end = 0
    events = sorted(event_partials_5m.items())
    count = 0
    result = {}
    for query_hop_start in sorted(queries_5m):
        while start < len(events) and events[start][0] < query_hop_start - 7 * millis_in_days:
            count -= events[start][1]
            start += 1
        while end < len(events) and events[end][0] < query_hop_start:
            count += events[end][1]
            end += 1
        result[query_hop_start] = count
    return result
```

Now we are ready to build up the logic of constructing windows - that is more amenable to distribution. The below code is very simplified and lacks notion of keys - like `user_id`. It is simply indexed by timestamps like before.

The below snippet is designed to be highly distributable. So each step is evenly split across multiple machines. It is a bunch of stages, but each stages is guaranteed to hold no more than 5 minutes worth of raw events - to handle skew.

```python
event_paritals_5m = reduce_by(events, hop_5m)
query_raw_5m = group_by(queries, hop_5m)
event_raw_5m = group_by(events, hop_5m)

window_head_input_5m = join(query_raw_5m, event_raw_5m)
window_heads = map_values(window_head_input, cumulative_merging_sum)
window_tails = compute_tails(query_raw_5m.keys(), event_partials_5m)

heads_and_tails = join(window_heads, window_tails)
for (query_head_ts, (query_heads, tail)):
    for (query_ts, head) in query_heads:
        yield query_ts, head + tail
```

That's it.

To recap, the intuition behind the optimizations. The biggest problems we are solving are:

1. Remove all `query*view_event` complexity compute from topology. We leverage sorting heavily to convert complexity to `q*log(q) + v*log(v)`.
1. Handle skew, by distributing data not just by keys, but by timestamps. We chose 5 minutes in the example, but this is configurable.

## Answers

### Naive Window Tiling Complexity

- [ n^2/k ]
- If a key happens to have just 10k queries and views, it will result in 100M operations for computing the result for just that key.
- If the data is Poisson distributed by the key, which is usually the case in a significant percentage of datasets, the job becomes significantly large.
- In a distributed setting, this will manifest as a set of slow executors (eg., spark workers) that never seem to complete.
- Switching between frameworks like Spark/Presto/Clickhouse/StarRocks - will optimize for the constants surrounding the complexity equation, but they will not solve the core problem.

## Scala interval tree code listing

Given two indices `left`, `right`, break it down into `log(n)` non overlapping but fully covering tiles. The imporant property being that any such query will produce a `log(n)` subset from the same `n` tiles. Which is what enables the re-use.

Below is a scala code listing, that produces tiles for a `tileConsumer`.
`tileConsumer` is a function that would perform the aggregation/update. Or for testing/benchmarking purposes count the number of tiles for various ranges etc.,

```scala
def generateTiles(left: Int, right: Int, tileConsumer: (Int, Int) => Unit): Int = {
   // find m, i such that
   // (m + 1) * (2 power i) < left <= m * (2 power i) <= right < (m + 1) * (2 power i)
   val powerOfTwo = 1 << (31 - Integer.numberOfLeadingZeros(left ^ right))
   val splitPoint = (right/powerOfTwo) * powerOfTwo
   // tiles on the left side
   var leftDistance = splitPoint - left
   var rightBoundary = splitPoint
   while(leftDistance > 0) {
     val maxPower = Integer.highestOneBit(leftDistance)
     tileConsumer(rightBoundary - maxPower, rightBoundary)
     rightBoundary -= maxPower
     leftDistance -= maxPower
   }
   // tiles on the right side
   var rightDistance = right - splitPoint
   var leftBoundary = splitPoint
   while(rightDistance > 0) {
     val maxPower = Integer.highestOneBit(rightDistance)
     tileConsumer(leftBoundary, leftBoundary + maxPower)
     leftBoundary += maxPower
     rightDistance -= maxPower
   }
   splitPoint
 }
```