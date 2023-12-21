
# Tiled Architecture

**Disclaimer**: Tiling is an experimental feature. It is used in production in some companies, but still in the process of being open-sourced.

## What is tiling?

Short summary. Intended for companies operating at significant scale.

Requires Flink

The regular, untiled version
- Architecture, image

The tiled version
- Architecture, image

## When should you use tiling?

If you need performance, or don’t have a KV store with range queries


## How to enable tiling

Look at the Flink code.

Reference PRs.


Please leave feedback in slack.

## Full end-to-end example

This example will show 

## Flink by example Flink work

Operator by operator. Examples

Counting the number of ice cream cones a person has bought in the last 6 hrs.
Keep track of the last ice cream flavour a person had in the last 6 hrs.


```python
ice_cream_source = ttypes.Source(
    events=ttypes.EventSource(
        query=Query(
            selects=select(
                customer_id="customer_id",
                flavour="ice_cream_flavour",
            ),
            time_column="created",
            …
        )
    )
)
```

```json
{
    created: 1000,
    customer_id: Alice,
    ice_cream_flavour: chocolate,
    ice_cream_cone_size: large
}
```

```python
ice_cream_group_by = GroupBy(
    sources=ice_cream_source,
    keys=["customer_id"],
    aggregations=[
        Aggregation(
            input_column="customer_id",
            operation=Operation.COUNT,
            windows=[
                Window(length=6, timeUnit=TimeUnit.HOURS),
            ],
        ),
        Aggregation(
            input_column="flavour",
            operation=Operation.LAST,
            windows=[Window(length=6, timeUnit=TimeUnit.HOURS)],
        ),
    ],
    accuracy=Accuracy.TEMPORAL,
    online=True,
    cluster=["flink-cluster-name"],
    environment=["qa", "production"],
…
)
```

```scala
// Input
IceCreamEventProto(
    customer_id = "Alice",
    created = 1000L,
    ice_cream_flavour = "chocolate",
    ice_cream_cone_size =  "large" // Not used 
)
// Output
Map(
    "customer_id" -> "Alice",
    "created" -> 1000L,
    "flavour" -> "chocolate"
)
```

### Fetcher side

From batch KV store
```
[-3:00, 0:00) -> [4, "chocolate"]
```
From online KV store
```
[0:00, 1:00) -> [1, "chocolate"]
[1:00, 2:00) -> [2, "vanilla"]
[2:00, 3:00) -> [2, "lemon"]
```
Combine everything and finalize: `[9, "lemon"]`. 🎉

