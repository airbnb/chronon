# CHIP-11: UnionJoin for Sequence Generation

_Author: Abby Whittier, Nikhil Simha

This CHIP introduces UnionJoin, an optimized join algorithm for sequence feature generation in Chronon.

## Motivation

Sequence features are a critical component of modern machine learning systems, but generating them efficiently at scale remains challenging. The standard join approach processes each label event independently, leading to redundant computation when generating sequences for multiple events with overlapping time windows.

The primary goal of this CHIP is to introduce an optimized join algorithm that takes advantage of the natural grouping in sequential data to achieve order-of-magnitude performance improvements for sequence feature generation workloads.

## Code Reference

Shout out to @nikhil-zlai for the implementation!

Most of my work was massaging into the Airbnb commit history.

https://github.com/zipline-ai/chronon/blob/main/spark/src/main/scala/ai/chronon/spark/join/UnionJoin.scala

## How It Works

### What is a UnionJoin?

A UnionJoin is an incremental compute algorithm that takes advantage of the natural grouping of sequential data.

**Traditional Join Approach:**

In a normal join algorithm, each label event, denoted as (user, timestamp, label), is processed independently. Each event joins with the feature event stream, and the corresponding feature data is scattered across different rows, creating a cross product. This is reliable and generic, but leads to the same sequences being recomputed multiple times.

For example, if you have 1000 label events for the same user within a 30-day window, the scatter-join will compute overlapping 30-day sequences 1000 times, with massive redundancy. You can think of this like an O(N²) solution to a sliding window problem on LeetCode.

```
Label Events:           Feature Events:
user1, t1, label1  -->  user1, t0, feature_a
user1, t2, label2  -->  user1, t5, feature_b
user1, t3, label3  -->  user1, t10, feature_c
                        ...
Each label independently joins with features
→ Same feature sequences computed multiple times
```

**UnionJoin Approach:**

A UnionJoin gathers all related keys, both on the label side and feature side, into a single partition. We then use a sliding window aggregation algorithm to generate sequences in O(N) time and space complexity using queue/dequeue operations.

```
1. Gather all events for user1:
   - All label events: (t1, label1), (t2, label2), (t3, label3), ...
   - All feature events: (t0, feature_a), (t5, feature_b), (t10, feature_c), ...

2. Sort by timestamp

3. Use sliding window algorithm to generate sequences:
   - Process events in order
   - Maintain window state with queue/dequeue pattern
   - Generate sequence for each label event

→ Each feature event processed once
```


### Dealing with Skew

The implementation is very sensitive to skewed keys, such as skewed queries/skewed rows.

Adaptive Query Engine (AQE) was a significant help in handling skew by dynamically splitting large partitions. 

PR here: https://github.com/airbnb/chronon/pull/1001

Incremental aggregation will be the primary next step for handling very long windows and high-cardinality keys.

### Limitations

- Lack of time bucketing means the relatively scaling is a bit worse. We find it ~linear with window size.
- Spark can only hold ~2 GB in a single column, so the "tail" can not exceed this length.

## Results

UnionJoin was 5-10x faster than the existing join algorithm for sequence feature generation within Netlfix compared to the existing.

## UX

We expose a spark param for which join algorithm to use. It is configured in `teams.json`

```
spark.chronon.join.backfill.mode.skewFree=true
```

Once we land incremental aggregation, we hope to use UnionJoin exclusively.