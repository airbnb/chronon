# The Tiled Architecture

## What is tiling?

Tiling, or the tiled architecture, is a modification to Chronon's online architecture to store pre-aggregates (also
known as "IRs" or Intermediate Representations) in the Key-Value store instead of individual events.

The primary purpose of tiling is to improve the handling of hot keys, increase scalability, and decrease feature serving
latency.

Tiling requires [Flink](https://flink.apache.org/).

### Chronon without tiling

The regular, untiled version works as pictured in Figure 1.

- The "write" path: reads an event stream, processes the events in Spark, then writes them out to a datastore.
- The "read" path: reads O(events) events from the store, aggregates them, and returns the feature values to the user.

![Architecture](../images/Untiled_Architecture.png)
_Figure 1: The untiled architecture_

At scale, aggregating O(n) events each time there is a request can become costly. For example, if you have an event
stream producing 10 events/sec for a certain key, a request for a feature with a 12-hour window will have to fetch and
aggregate 432,000 events every single time. For a simple GroupBy that counts the number of events for a key, Chronon
would iterate over 432,000 items and count the total.

### Chronon with tiling

The tiled architecture, depicted in Figure 2, works differently:

- The "write" path: reads an event stream, processes and pre-aggregates the events in a stateful Flink app, then writes
  out the pre-aggregates to "tiles" in the store.
- The "read" path: reads O(tiles) tiles from the store, merges the pre-aggregates, and returns the feature values to the
  user.

![Architecture](../images/Tiled_Architecture.png)
_Figure 2: The tiled architecture_

Tiling shifts a significant part of the aggregation work to the write path, which allows for faster feature serving.

Using the same example as above (an event stream producing 10 events/sec for a certain key, and a GroupBy with a 12-hour
window), a request for feature values would fetch and merge 12 or 13 1-hour tiles. For a simple GroupBy that counts the
number of events for a key, Chronon would iterate over 13 numbers and add them together. That's significantly less work.

#### Example: Fetching and serving tiled features

Suppose you have a GroupBy with two aggregations, `COUNT` and `LAST`, both using 3-hour windows, and you are storing
1-hour tiles in KV Store. To serve them, the Chronon Fetcher would fetch three tiles:

```
[0:00, 1:00) -> [2, "B"]
[1:00, 2:00) -> [9, "A"]
[2:00, 3:00) -> [3, "C"]
```

Then, it would combine the IRs to get the final feature values: `[14, "C"]`.

## When to use tiling

In general, tiling improves scalability and decreases feature serving latency. Some use cases are:

- You want to decrease feature serving latency. At Stripe, migrating to tiling decreased serving latency by 33% at 4K
  rps.
- You don't have access to a datastore with range queries
- You want to reduce fanout to your datastore.
- You need to support aggregating over hot key entities

In particular, organizations operating at significant scale with many hot-key entities should consider using the tiled
architecture. If the number of events per entity key is at most a few thousand, the untiled approach would still perform
well.

## How to enable tiling

To enable tiling, you first need to start using Flink on the write path. See
the [Chronon on Flink documentation](setup/Flink.md) for instructions. As part of this process, you may also need to
modify your KV store implementation to know how to write and fetch tiles.

Once the Flink app is set up and writing tiles to your datastore, the final step is to enable tiled reads in the
Fetcher. Just add `enable_tiling=true` to
the [customJson](https://github.com/airbnb/chronon/blob/48b789dd2c216c62bbf1d74fbf4e779f23db541f/api/py/ai/chronon/group_by.py#L561)
of any GroupBy definition. 


## Advanced Options

### Tile Layering

Tile layering is an optimization that introduces 1-hour tiles alongside 5-minute tiles for GroupBys with windows shorter than 12 hours. This reduces the number of tiles fetched from the key-value store (by ~85% in practice), resulting in a substantial decrease in feature serving latency. Tile layering is enabled by default when using the tiled architecture.

#### How It Works

**Tile Size Selection Before Tile Layering**

Without tile layering, tile sizes in Flink are determined by the three default resolutions used in Chronon:

* 5-minute tiles: For GroupBys with any window shorter than 12 hours
* 1-hour tiles: For GroupBys with the shortest window between 12 hours and 12 days
* 1-day tiles: For GroupBys with the shortest window of 12 days or longer

For instance, a GroupBy with windows of 1 hour, 12 hours, 7 days, and 30 days would use 5-minute tiles due to the 1-hour window.

(Note: These resolutions are uses all across Chronon for sawtooth window calculation. For more details, see [Windowing](https://chronon.ai/authoring_features/GroupBy.html#windowing) and [Resolution.scala](https://github.com/airbnb/chronon/blob/main/aggregator/src/main/scala/ai/chronon/aggregator/windowing/Resolution.scala#L40-L42))

**Tile Size Selection With Tile Layering**

For GroupBys with windows shorter than 12 hours:

1. The Flink job incorporates two window operators: one for 5-minute tiles and another for 1-hour tiles.
2. Both 5-minute and 1-hour tiles are stored in the key-value store.
3. The Fetcher prioritizes larger tiles when possible, using smaller tiles only when necessary. For GroupBys with multiple window sizes, overlapping tiles may be fetched, but each window size is calculated independently and there is no double-counting.

#### Example: Fetching a GroupBy with 6-hour and 1-day Windows at 14:34

(Assume batch data for the previous day has already landed, so we only need tiles starting at 00:00.)

**Before Tile Layering**

Without tile layering, the system fetches 175 5-minute tiles from the KV store:

```
[0:00 - 0:05), [0:05 - 0:10), … , [14:30 - 14:35)
```

To aggregate the values for the 1-day window, the Fetcher uses all these tiles. For 6-hour window, it uses only those from 8:30 onwards.

**After Tile Layering**

With tile layering, the system fetches:

```
5-minute tiles: [8:30 - 8:35) ... [8:55 - 9:00)
1-hour tiles: [0:00 - 1:00) ... [9:00 - 15:00)
```

In total, 21 tiles are fetched: 6 five-minute tiles (8:30 to 9:00) and 15 one-hour tiles (0:00 to 15:00). The [14:00 - 15:00) tile is partially complete, containing data from 14:00 to 14:34.

For the 6-hour window, tiles from 8:30 to 14:34 are aggregated. For the 1-day window, one-hour tiles from 0:00 to 14:34 are used.

### Impact

Tile layering has several benefits:

1. Reduced latency: Fetching fewer, larger tiles significantly decreases serving time. Users can expect improvements of at least 50% at p99.
2. Lower CPU utilization: Less computational work is required to deserialize and merge fewer tiles.
3. Decreased data store fanout: The number of requests to the store is substantially reduced.

However, there are trade-offs to consider:

1. Increased Flink workload: Jobs must perform more work and maintain more state. However, Stripe's experience with tile layering over more than a year suggests that state size has not become a bottleneck. 
Moreover, the windowing portion of the Flink DAG (which is duplicated with Tile Layering) tends to consume less CPU compared to the initial, shared part of the DAG (responsible for event fetching, deserialization, and Spark expression evaluation). As a result, scaling up Flink jobs is often unnecessary with Tile Layering.


2. Higher key-value store utilization: Writing additional tiles increases storage requirements.
