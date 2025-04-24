# Context

Chronon reads data partitions based on window sizes to compute features for event sources. For example

sum_clicks_past_30days          ←  30 day window
count_visits_past_90days         ←  90 day window

When computing aggregations, one of the major time taking tasks is reading data from disk. If a feature needs 90 days lookback, Chronon reads 90 days worth of data to compute. When this feature is computed in production everyday, consecutive days have 89 partitions in common.

<img src="./images/CHIP10_inc_agg_visual" alt="Incremental Agg" width="300" />


This CHIP is to add support for incremental aggregation which will avoid reading 89 common partitions between consecutive days and take advantage of previous day’s computation in feature store.


# Usecases
## Case 1 : Unwindowed deletable


> Eg:  how many listings purchased by the user since signup.

These features are not windowed, which means the feature calculation happens over the lifetime.

<img width="595" alt="Image" src="https://github.com/user-attachments/assets/78f2a0f3-5b0a-4fd2-b3a6-cfc1682feaa3" />

## Case 2 : Windowed deletable
These features are windowed and has a inverse operator

> Eg: Number of listings(Count Aggregation) purchased by the user in the past 90 days

<img width="586" alt="Image" src="https://github.com/user-attachments/assets/dd2ab58b-91e5-41b8-b8dd-4294e68a0116" />

<img width="606" alt="Image" src="https://github.com/user-attachments/assets/4bdef69b-2af4-40e6-b430-894ee6540eaa" />

To compute above groupBy incrementally
* Read the output table from groupby to get previous day’s aggregated values
* Read _day 0_ to add the latest activity
* Read user activity on day 4, day 11 to delete the unwanted user data

## Case 3: Windowed non-deletable

These features are windowed and does not have an inverse operator

_

> Eg: What is the max purchase price by the user in the past 30 days

_

For non-deletable operators, we have to go with the current behavior of Chronon where we load all the data/partitions needed to compute feature. 
