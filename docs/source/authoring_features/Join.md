# Join

As the name suggests `Join`, it is primarily responsible for joining together many `GroupBy`s, possibly with different keys. However, it is also responsible for another very important function: defining the timeline along which features will be computed in the backfill.

Let's use an example to explain this further. In the [Quickstart](TODO) guide, we define some features as aggregations of user's purchases and returns, as well as some other user dimensions like whether their accounts are verified. We intend to use these features in an online fraud model that runs at **checkout time**.

This is important because it means that when we serve the model online, inference will be made at checkout time, and therefore when we backfill features for training data, we want every row to correspond to a historical checkout event, and features should be computed as of the timestamps that those events occurred. I.e. every row of training data for the model has identical feature values to what the model would have seen had that row been on a production inference request.

To see how we do this, let's take a look at the left side of the join definition (taken from [Quickstart Training Set Join](https://github.com/airbnb/chronon/blob/master/api/py/test/sample/joins/quickstart/training_set.py)).

```python
source = Source(
    events=EventSource(
        table="data.checkouts", 
        query=Query(
            selects=select("user_id"), # The primary key used to join various GroupBys together
            time_column="ts",
            ) # The event time used to compute feature values as-of
    ))

v1 = Join(  
    left=source,
    right_parts=[JoinPart(group_by=group_by) for group_by in [purchases_v1, returns_v1, users]] # Include the three GroupBys
)
```

Key points:
* The `left` source is built on top of the checkouts data source. This is driven by the use that we're modeling. In this case we're trying to predict whether checkout events are fraudulent or not, so we use checkouts as the left.
* The `left` side selects only the user_id field, because all of our `GroupBy`s that we use in the right parts are based off user. If we also had a `GroupBy` keyed off of some different key, like `browser` or `ip_address`, then the left side would also need to provide those keys.
* The `right_parts` specifies the `GroupBy`s that we wish to use.

Here is what one row of sample output would look like after running this join:


```
user_id                                            | 24
ts                                                 | 1701320475364
quickstart_purchases_v1_purchase_price_sum_3d      | 331
quickstart_purchases_v1_purchase_price_sum_14d     | 1574
quickstart_purchases_v1_purchase_price_sum_30d     | 1934
quickstart_purchases_v1_purchase_price_count_3d    | 1
quickstart_purchases_v1_purchase_price_count_14d   | 4
quickstart_purchases_v1_purchase_price_count_30d   | 5
quickstart_purchases_v1_purchase_price_average_3d  | 331.0
quickstart_purchases_v1_purchase_price_average_14d | 393.5
quickstart_purchases_v1_purchase_price_average_30d | 386.8
quickstart_purchases_v1_purchase_price_last10      | [331, 474, 497, 272, 360]
quickstart_returns_v1_refund_amt_sum_3d            | null
quickstart_returns_v1_refund_amt_sum_14d           | 76
quickstart_returns_v1_refund_amt_sum_30d           | 645
quickstart_returns_v1_refund_amt_count_3d          | null
quickstart_returns_v1_refund_amt_count_14d         | 1
quickstart_returns_v1_refund_amt_count_30d         | 3
quickstart_returns_v1_refund_amt_average_3d        | null
quickstart_returns_v1_refund_amt_average_14d       | 76.0
quickstart_returns_v1_refund_amt_average_30d       | 215.0
quickstart_returns_v1_refund_amt_last2             | [76, 388]
quickstart_users_v1_account_created_ds             | 2023-07-01
quickstart_users_v1_email_verified                 | 0
ds                                                 | 2023-11-30
```

The first two columns, `user_id` and `ts` are provided by the `left` side of the join, and the remaining columns are backfilled by the join compute engine. The windowed aggregations, i.e. `refund_amt_sum_30d` are accurate as of the precise millisecond of the `ts` on the left side of the join.

## Orchestration

Once the join is merged, Chronon runs the following jobs:

* Daily front-fill of new feature values as upstream data lands in the source tables.
* If online serving is enabled, then Chronon runs pipelines that measure consistency between an offline join, and an online joins. These output metrics can be used to 

These jobs are managed by airflow pipelines (see [Orchestration](TODO) documentation).

## Source in Join

`left` source is the driver for feature backfills. **It only matter for offline backfilling and not used in serving.** In online serving the fetcher takes a list of primary keys to fetch feature values for, which resembles the `left` source, however, it does not require a timestamp (becasue the online Fetcher always assumes that you want the most up to date feature values, i.e. timestamp=now).

In the above example, the left source is an `EventSource`, however, in some cases it can also be an `EntitySource`. In both cases, however, it will never be streaming. This is because streaming is a strictly online concepts (realtime updates to the KV store), whereas the `left` source is only ever used to drive offline backfills.

Using an `EntitySource` will result in meaningfully different results for feature computation, primarily because `EntitySource`s do not have a `time` column. Rather, `EntitySources` have daily snapshots, so feature values are computed as of midnight boundaries on those days.

See the [Computation examples](#computation-examples) for illustrations of how these source types interact with feature computation.
 
## KeyMapping and Prefix

`prefix` adds the specified string to the names of the columns from group_by.

`keyMapping` is a map of string to string. This is used to re-map keys from left side into right side. You could have 
a group_by on the right keyed by `user`. On the left you have chosen to call the user `user_id` or `vendor`. Then you
can use the remapping facility to specify this relation for each group_by.

TODO: examples

## Online and Production flags

TODO

## Label Join

TODO

## Derivations

TODO

## External Features

TODO


# Computation examples

## Left side events, right side streaming events

TODO

## Left side events, right side batch events

TODO

## Left side entities, right side realtime entities

TODO

## Left side entities, right side batch entities

TODO
