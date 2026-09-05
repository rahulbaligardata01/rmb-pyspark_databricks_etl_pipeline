# Assignment A7 - Performance Notes

## 1. Objective

This document summarizes the performance analysis performed for the PySpark clickstream ETL pipeline.

The analysis compares:

- DataFrame `groupBy`
- RDD-based `reduceByKey`
- Window functions

It also documents observations from Spark physical execution plans using `explain()`.

## 2. Environment

- Platform: Databricks Free Edition
- Compute: Serverless
- Spark version: 4.2.0
- Execution engine: Databricks Photon
- Language: PySpark
- Input format: JSON
- Output format: Partitioned Parquet

## 3. Dataset and Benchmark Limitation

No sample clickstream data was provided for the assignment, so a small synthetic dataset was created to validate the ETL logic.

The dataset contains:

- Multiple users and user segments
- Multiple sessions
- Multiple dates
- Duplicate events
- Missing timestamps
- An unknown user
- Single-event sessions for bounce analysis

The development dataset is intentionally small and is therefore not representative of the assignment's target workload of approximately 50 GB over six months.

Consequently, runtime measurements on this dataset should not be interpreted as production-scale benchmarks.

The performance analysis focuses primarily on:

- Shuffle behavior
- Physical execution plans
- Sorting requirements
- Aggregation strategy
- API suitability
- Expected scalability characteristics

## 4. groupBy Analysis

The pipeline uses DataFrame `groupBy` for session-level and daily aggregations.

Example:

```python
groupby_result = (
    events_valid_df
    .groupBy("user_id")
    .count()
)
```

The physical plan showed:

```text
hashpartitioning(user_id, 16)
```

and included:

```text
PhotonShuffleExchangeSink
```

This indicates that Spark redistributes records by `user_id` so that records belonging to the same grouping key can be processed together.

The plan also showed another shuffle on:

```text
hashpartitioning(event_id, 16)
```

which was associated with `dropDuplicates("event_id")`.

### Observation

`groupBy` is appropriate when the required result is an aggregated dataset with fewer rows, such as:

- events per user
- sessions per segment
- daily metrics

At larger data volumes, shuffle cost becomes an important performance consideration.

## 5. reduceByKey Analysis

`reduceByKey` is an RDD-based aggregation operation.

Conceptually:

```python
rdd.reduceByKey(lambda a, b: a + b)
```

Its important optimization is map-side combining.

Conceptually:

```text
Input records
     |
Local aggregation
     |
Smaller data volume
     |
Shuffle
     |
Final reduction
```

This can reduce the amount of intermediate data transferred during the shuffle compared with approaches that move all values before aggregation.

A direct `reduceByKey` benchmark was not executed because Databricks Free Edition serverless restricts direct access to the SparkContext/RDD API.

Therefore, `reduceByKey` is included as a conceptual performance comparison rather than as a measured runtime result.

## 6. Window Function Analysis

Window functions were used for sessionization.

The window specification was:

```python
user_time_window = (
    Window
    .partitionBy("user_id")
    .orderBy("event_timestamp")
)
```

`lag()` was then used to obtain the previous event timestamp.

The physical plan showed:

```text
PhotonShuffleExchangeSink
hashpartitioning(user_id, 16)

        ↓

PhotonSort
[user_id, event_timestamp]

        ↓

PhotonWindow
lag(...)
```

### Observation

The window operation requires the data to be organized by `user_id` and ordered chronologically within each user.

Therefore, the physical plan can require both:

1. Redistribution/shuffle
2. Sorting

This can make window operations more expensive than simple aggregations when processing large datasets.

However, window functions are appropriate when calculations depend on relationships between rows while retaining row-level detail.

## 7. Physical Plan Analysis

### groupBy

The `groupBy("user_id").count()` physical plan included:

```text
PhotonGroupingAgg
PhotonShuffleExchangeSink
hashpartitioning(user_id, 16)
```

This indicates a shuffle before the final aggregation.

The plan also contained an earlier shuffle caused by event deduplication:

```text
hashpartitioning(event_id, 16)
```

### Window

The sessionization plan included:

```text
hashpartitioning(user_id, 16)
Sort(user_id, event_timestamp)
PhotonWindow
```

This confirms that the window calculation requires data redistribution and ordering.

### Adaptive Query Execution

The physical plans begin with:

```text
AdaptiveSparkPlan
```

indicating that Adaptive Query Execution (AQE) is involved.

### Photon

The plans also show Photon operators and state:

```text
The query is fully supported by Photon.
```

## 8. Performance Comparison

| Approach | Primary use | Shuffle | Sorting | Result shape |
|---|---|---|---|---|
| `groupBy` | Aggregation | Often required | Usually not inherently required for aggregation | Fewer rows |
| `reduceByKey` | RDD key aggregation | Required | Not inherently required | Fewer rows |
| Window | Row-context calculations | Can be required | Often required when ordering is specified | Retains rows |

### Practical selection

Use `groupBy` when the objective is to reduce records into aggregate results.

Use `reduceByKey` when working with RDDs and key-based reductions where map-side combining is beneficial.

Use window functions when the calculation depends on neighboring or ordered records while retaining the original row-level information.

## 9. Important Spark Performance Observations

### Shuffle

`Exchange` / `ShuffleExchange` in a physical plan is an important indicator of data redistribution.

Shuffles can introduce:

- Network transfer
- Serialization/deserialization
- Disk I/O
- Additional task overhead

Therefore, minimizing unnecessary shuffles is an important optimization goal for large workloads.

### Sorting

Window functions that specify ordering can require sorting within partitions.

Sorting can become expensive as the amount of data increases.

### Partition count

The observed physical plans used:

```text
hashpartitioning(..., 16)
```

for the relevant shuffle operations.

The optimal shuffle partition count depends on workload size, cluster resources, and data distribution. It should therefore be evaluated rather than blindly increased or decreased.

## 10. Scaling Considerations for the ~50 GB Scenario

The development dataset is much smaller than the assignment's stated ~50 GB workload.

At production scale, important considerations would include:

- Avoiding unnecessary shuffles
- Choosing appropriate shuffle partition counts
- Broadcasting genuinely small dimension datasets
- Detecting and mitigating data skew
- Using efficient columnar formats such as Parquet
- Partitioning output by useful filtering columns such as date
- Avoiding unnecessary wide transformations
- Monitoring Spark UI stages and task distribution
- Reviewing physical plans using `explain()`

The user-dimension join is also a potential candidate for a broadcast join when the dimension is sufficiently small, which can avoid a large shuffle join.

## 11. Limitations

The main limitations of this exercise are:

1. The clickstream dataset is synthetic and small.
2. It does not represent the full six months / ~50 GB workload.
3. No direct `reduceByKey` runtime benchmark was possible in the Databricks Free Edition serverless environment.
4. Runtime values from the development dataset would not be meaningful production-scale benchmarks.

Therefore, performance conclusions are based primarily on Spark execution plans and execution characteristics rather than claiming a measured 50 GB runtime improvement.

## 12. Conclusion

The analysis shows that `groupBy` is appropriate for reducing event data into aggregate results, while window functions are appropriate for ordered, row-level calculations such as sessionization.

The physical plans demonstrate that both approaches can involve shuffle operations, while window functions can additionally require sorting according to their ordering specification.

`reduceByKey` can benefit from map-side combining for key-based RDD aggregation, but direct RDD execution was not benchmarked because of the current serverless environment limitations.

For a production-scale ~50 GB clickstream workload, minimizing unnecessary shuffles, controlling partition sizes, selecting appropriate join strategies, and monitoring physical execution plans would be key performance considerations.
