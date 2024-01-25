# Chaining Features

## What is a Chaining Feature
Chaining Features refers to features which require weaving together transformations and aggregations along with denormalization.
For example, compute  “average square footage of last_k listings viewed by a user in the last week” or “average price of listings a user viewed in the last 14 days”.
These features cannot be expressed in single Join or GroupBy but need to be aggregated in several steps.

Assuming that square footage of a listing is stored separately from listing views - computing this requires
- computing last_k listings viewed per user first from a view stream and
- enriching the listing_ids again with listing price
- finally aggregating the listing price.

To express the above transformations, we need 2 Joins in Chronon to aggregate listings in the last 14 days and average price for these listings.
Chronon now has the capability to use output of join as input to downstream computations and serve chaining features in real-time, enabling users to seamlessly access the features without worrying about upstream computation. In detail, Chronon enriches data in following three computation paradigms to provide efficient and accurate results.
- Batch: Enrich a hive table containing search events with this feature to create a training set.
- Streaming: Enrich a stream of search events in real-time with this feature for ML feature serving.
- Application services: Synchronously respond to a “search request” with this feature as a response.

If you have similar features which would require multiple joins or groupbys, chaining features might be a good fit for your use case.

## How do I use it?
We introduced a new source type **JoinSource**. You can now pass in a parent join as Source in GroupBys.

```python 
# Chaining Feature API example 

# Upstream Join. Regular chronon Join for last prices of listings
parent_join = Join(
  left=rating_driver,
  right_parts=[
    JoinPart(group_by=listing_price_last),
  ],
  online=True,
  realtime=True,
  output_namespace="chronon_test",
  team_override='ml_infra',
)

listing_price_last = GroupBy(
  name="chronon_test.join_right_part",
  sources=price_mutations,
  keys=["listing"],
  aggregations=[
    Aggregation(input_column="price", operation=Operation.LAST),
  ],
  output_namespace="chronon_test",
  online=True,
  team_override='ml_infra',
  accuracy=Accuracy.TEMPORAL,
)

# Downstream Groupby for avg price of listings viewed in last 14 days per user
# New API in bold
enriched_listings = Join(
  left = source_table,
  right_parts = [
    JoinPart(
      group_by = GroupBy(
        sources = JoinSource(join = parent_join, query=Query(...)),
        keys = ["user"],
        aggregations = [
          Aggregation(
            operation = Operation.LAST_K(100),
            input_column = "price_last",
            window = [Window(14, TimeUnit.DAYS)]
          )
        ],
        accuracy = Accuracy.TEMPORAL
      ))
  ],
  derivations = [
    Derivation(
      name = "avg_listing_view_price_last_14d",
      expression = "avg(enriched_views_price_last_last_100)"
    )
  ]
)

```
### Configuration Example
[Chaining GroupBy](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/group_bys/sample_team/sample_chaining_group_by.py)

[Chaining Join](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/joins/sample_team/sample_chaining_join.py)

## Clarifications
- The goal of chaining is to use output of a Join as input to downstream computations like GroupBy or a Join. As of today we support the case 1 and case 2 in future plan
    - Case 1: A Join output is the source of another GroupBy
    - Case 2: A Join output is the source of another Join – To be supported 



