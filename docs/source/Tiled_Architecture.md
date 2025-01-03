# The Tiled Architecture

**Important**: Tiling is a new feature currently being open-sourced. Feel free to reach out in the Chronon Slack channel if you need support using it.

## Tiling Overview

The tiled architecture is a change to Chronon's online architecture that optimizes data storage and retrieval by utilizing pre-aggregates (Intermediate Representations or IRs) in the Key-Value store instead of individual events.

Tiling is primarily designed to improve the handling of hot keys, increase system scalability, and decrease feature serving latency.

Tiling requires [Flink](https://flink.apache.org/) for stream processing.

### Traditional Chronon Architecture

The regular, untiled architecture (Figure 1) works the following way:

- Write path: ingests an event stream, processes the events, and writes them out individually to a datastore.
- Read path: retrieves O(events) from the store, aggregates them, and returns feature values to the client.

![Architecture](../images/Untiled_Architecture.png)
_Figure 1: Regular, untiled Chronon architecture_

At scale, aggregating O(n) events per request can become computationally expensive. For example, with an event stream generating 10 events/sec for a specific key, each feature request with a 12-hour window requires fetching and aggregating 432,000 events. For a simple GroupBy that counts the number of events for a key, Chronon would iterate over 432,000 items to compute the total.

### Tiled Chronon Architecture

The tiled architecture (Figure 2) operates as follows:

- Write path: ingests event stream, processes and pre-aggregates events using a stateful Flink app, and writes out pre-aggregates as "tiles" in the store.
- Read path: retrieves O(tiles) tiles from the store, merges pre-aggregates, and returns feature values to the client.

![Architecture](../images/Tiled_Architecture.png)
_Figure 2: Tiled Chronon architecture_

This architecture shifts a significant part of aggregation work to the write path, allowing faster feature serving.

Using the same example as above (10 events/sec, 12-hour window GroupBy), a request for feature values in this architecture would only fetch and merge 12 or 13 1-hour tiles. For a simple count-based GroupBy, Chronon would iterate over 13 pre-computed sums and add them together. That's significantly less work.

#### Example: Fetching and serving tiled features

Consider a GroupBy with `COUNT` and `LAST` aggregations using 3-hour windows, with 1-hour tiles in the KV Store. To serve these features, the Chronon Fetcher would retrieve three tiles:

```
[0:00, 1:00) -> [2, "B"]
[1:00, 2:00) -> [9, "A"]
[2:00, 3:00) -> [3, "C"]
```

The IRs would then be combined to produce the final feature values: `[14, "C"]`.

## Tiling Use Cases

Tiling is particularly beneficial for:

1. Reducing feature serving latency at scale (Stripe saw a 33% reduction after the initial implementation.)
2. Minimizing datastore load, as fetching O(tiles) is generally less work than fetching O(events).
3. Supporting aggregations over very hot key entities
4. Organizations that don't have access to a datastore with range query capabilities, such as Cassandra. Tiling allows efficient retrieval of aggregated data without the need for complex range queries.

Organizations operating at scale with many hot-key entities (such as big merchants in payments companies) should consider using the tiled architecture. If your hottest keys don't exceed a few thousand events per day, the untiled approach may still be sufficient.

## How to enable tiling

To enable tiling:

1. Implement Flink on the write path (refer to the [Chronon on Flink documentation](setup/Flink.md)). As part of this process, you may also need to modify your KV store implementation to support writing and fetching tiles.
2. Once your Flink app is writing tiles to the datastore, enable tiled reads in the Fetcher by adding `enable_tiling=true` to the [customJson](https://github.com/airbnb/chronon/blob/48b789dd2c216c62bbf1d74fbf4e779f23db541f/api/py/ai/chronon/group_by.py#L561) of any GroupBy definition.