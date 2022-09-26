## Aggregations

### Supported aggregations
#### Simple Aggregations
count, mean, variance, min, max, top_k, bottom_k
#### Time based Aggregations
#### Sketching Aggregations
#### Reversible Aggregations

## Windowing
## Bucketing
## Lists as inputs


| Aggregation         | Supported Input Types | Output Type      | Reversibility  |    Space    |
| ------------------- | --------------------- | ---------------- | -------------- | ----------- |
| count               | All types             | Long             | Reversible     |   Bounded   |
| unique_count        | All Primitive types   | Long             | Non-Reversible |  Un-Bounded |
| approx_unique_count | All Primitive types   | Long             | Non-Reversible |   Bounded   |
| histogram           | String                | Map[String, Int] | Reversible     |  Un-Bounded |
| approx_percentile   | All Numeric types     | Map[String, Int] | Non-Reversible |   Bounded   |
| average             | All Numeric types     | Double           | Reversible     |   Bounded   |
| variance            | All Numeric types     | Double           | Non-Reversible |   Bounded   |
| Max                 | All Numeric types     | Double           | Non-Reversible |   Bounded   |
## Tunability
### Histogram
You can truncate the final result map to contain `k` entries representing top k counts. The intermediate maps are still unbounded.

### Approx Unique Count
Approx unique count is a sketching algorithm based on CPCSketch. The Sketch essentiallly is a trade off between
space and accuracy. You can tune this sketch to be more accurate or less by giving it more or less space, respectively.
Chronon currently doesn't expose the tuning param, but it can be done quite easily. Please file an issue if you need it.
The tuning is based on a error vs. size table [here](https://github.com/apache/incubator-datasketches-java/blob/master/src/main/java/org/apache/datasketches/cpc/CpcSketch.java#L180).

### Approx Percentile

For percentiles the underlying implementation is [KLLSketch](https://datasketches.apache.org/docs/KLL/KLLSketch.html). There are two additional inputs that can be given in the
`argMap` for percentiles:

* **k** (Optional, Int): Affects the accuracy and size of the sketch.
* **percentiles** (Required, String): Comma separated string of percentiles to be finalized (Ex: "0.25, 0.5, 0.75").

As such it's possible to add more percentiles to be finalized to an existing aggregation, as the intermediate result
stored is independent of this value.
