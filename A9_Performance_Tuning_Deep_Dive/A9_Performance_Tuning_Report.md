# A9: Performance Tuning Deep Dive

## 1. Objective

This assignment investigates common PySpark performance bottlenecks and demonstrates practical tuning techniques using a controlled synthetic workload.

The assignment scenario assumes a PySpark job that runs for approximately 4 hours. The implementation below reproduces the performance-tuning concepts locally using Apache Spark rather than a production-scale 50 GB workload.

The goals were to:

- identify and mitigate data skew using salting and Adaptive Query Execution (AQE),
- replace a shuffle-based join with a broadcast join,
- evaluate different values of `spark.sql.shuffle.partitions`,
- demonstrate caching/persistence of a reusable intermediate DataFrame,
- inspect and interpret Catalyst plans with `explain(True)`, and
- document measured runtime, resources, and Spark UI execution evidence.

---

## 2. Environment

| Item | Value |
|---|---|
| Spark version | 4.2.0 |
| Spark master | `local[*]` |
| Default parallelism | 8 |
| Final shuffle partition setting | 200 |
| AQE | Enabled |
| Execution environment | Local Windows development machine |

The benchmark was intentionally performed on a local synthetic dataset. Therefore, the measured runtimes are useful for comparing execution strategies in this experiment, but they should not be interpreted as production-scale runtime estimates for a 4-hour, 50 GB job.

---

## 3. Workload and Data Skew

A synthetic transaction dataset containing 10,000 records was created with a deliberately skewed account distribution:

| Account | Records | Share |
|---|---:|---:|
| A001 | 8,000 | 80% |
| A002 | 500 | 5% |
| A003 | 500 | 5% |
| A004 | 500 | 5% |
| A005 | 500 | 5% |

`A001` is the hot key and represents 80% of the input records. This creates the type of uneven workload distribution that can produce slow or straggler tasks in a shuffle-heavy Spark job.

---

# 4. Task 1 — Data Skew, Salting and AQE

## 4.1 Baseline skewed aggregation

The baseline aggregation grouped transactions by `account_id` and calculated transaction count and total amount.

**Measured runtime:** `3.5668 s`

The Spark UI was inspected to identify execution stages and task behavior.

### Spark UI evidence

- `01_baseline_skew_stage.png`

---

## 4.2 Salting

To distribute the hot `A001` key, a salt value with four buckets was added to the transaction records.

The resulting distribution was:

| Account | Salt 0 | Salt 1 | Salt 2 | Salt 3 |
|---|---:|---:|---:|---:|
| A001 | 2,000 | 2,000 | 2,000 | 2,000 |
| A002 | 128 | 128 | 124 | 120 |
| A003 | 128 | 128 | 124 | 120 |
| A004 | 128 | 128 | 124 | 120 |
| A005 | 128 | 128 | 124 | 120 |

The hot key therefore changed from one 8,000-record group to four 2,000-record salted groups. The smaller accounts are uneven only because 500 records cannot be divided evenly across four buckets.

A two-stage aggregation was then used:

1. aggregate by `account_id` and `salt`, and
2. remove the salt and combine the partial results by `account_id`.

The salted result was checked against the baseline to ensure that the optimization preserved the original business result.

**Measured salted aggregation runtime:** `4.1022 s`

Because the test dataset is very small and runs locally, the salted version was not expected to be faster in wall-clock time. The additional aggregation work can outweigh the benefit of improved distribution at this scale. The objective was to demonstrate the salting technique and its effect on workload distribution.

### Evidence

- `02_salting_distribution.png`
- `03_salted_aggregation_stage.png`

---

## 4.3 Adaptive Query Execution (AQE)

AQE was evaluated using a controlled skewed join.

For the comparison, the same shuffle-based join was executed twice:

- AQE disabled
- AQE enabled with skew-join handling enabled

### AQE comparison

| Configuration | Main stage | Tasks | Stage duration |
|---|---:|---:|---:|
| AQE OFF | Stage 51 | 8 | 13 s |
| AQE ON | Stage 56 | 8 | 12 s |

The AQE-on run was approximately 1 second faster in this small local experiment.

The Spark UI stage details showed approximately uniform task durations in both runs. The experiment therefore demonstrates the AQE configuration and comparison process, but it does not provide evidence of a large skew-handling gain at this small data volume.

### Evidence

- `04_skewed_join_aqe_off.png`
- `05_skewed_join_aqe_on.png`

### Interpretation

AQE allows Spark to use runtime statistics to adapt parts of the physical execution plan. Skew-join handling can identify unusually large join partitions and split them into smaller pieces. The benefit is workload-dependent and is expected to become more significant for larger shuffles and stronger skew.

---

# 5. Task 2 — Replace Shuffle Joins with Broadcast Joins

## 5.1 Shuffle join baseline

A small account dimension table containing five rows was joined with the transaction dataset. A merge hint was used to force a shuffle-based join strategy for the baseline.

The physical plan contained:

- `SortMergeJoin`
- `Exchange hashpartitioning(...)` on the join inputs

These `Exchange` operators indicate data redistribution/shuffle before the join.

**Measured runtime:** `15.3626 s`

### Evidence

- `06_shuffle_join_plan.png`

---

## 5.2 Broadcast join optimization

The five-row dimension table was explicitly broadcast using `broadcast(account_dimension_df)`.

The physical plan changed to:

- `BroadcastHashJoin`
- `BroadcastExchange`

This avoids the normal shuffle-based strategy for the small dimension table and allows Spark to perform the join using a broadcast copy of the small side.

**Measured runtime:** `13.8781 s`

### Measured improvement

Broadcast join improvement relative to the shuffle-join baseline:

**approximately 9.7% faster** in this local benchmark.

The Spark UI stage for the broadcast join showed zero shuffle-read bytes for the displayed tasks, which is consistent with the broadcast strategy avoiding the shuffle read associated with the join stage.

### Evidence

- `08_broadcast_join_plan.png`
- `09_broadcast_join_stage.png`

### Interpretation

Broadcast joins are most appropriate when one side is small enough to be safely replicated to the executors. Broadcasting a large table can create memory pressure, so this is not a universal optimization.

---

# 6. Task 3 — Tune `spark.sql.shuffle.partitions`

The local environment reported:

- Default parallelism: `8`
- Initial `spark.sql.shuffle.partitions`: `200`

For a controlled comparison, AQE was temporarily disabled so that adaptive coalescing would not obscure the effect of the configured partition count itself.

Three values were tested using the same aggregation workload.

| Shuffle partitions | Runtime |
|---:|---:|
| 8 | **7.0099 s** |
| 32 | **5.7479 s** |
| 200 | **5.9353 s** |

### Result

The best observed setting for this particular local workload was:

**32 shuffle partitions — 5.7479 s**

Compared with 200 partitions, the 32-partition configuration was approximately **3.2% faster**.

Compared with 8 partitions, it was approximately **18.0% faster**.

### Interpretation

The experiment demonstrates that the best value cannot be inferred simply from the number of available CPU threads. Too few shuffle partitions can produce larger tasks and insufficient parallelism, while too many partitions can add task scheduling and management overhead. The optimal setting is workload- and resource-dependent and should be benchmarked using representative data.

---

# 7. Task 4 — Cache / Persist Intermediate DataFrames

A reusable intermediate DataFrame was created by filtering transactions to `amount > 500` and selecting the required columns.

The experiment compared reuse without persistence against an explicitly cached version.

### Initial measurements without cache

| Action | Runtime |
|---|---:|
| First action without cache | 3.3860 s |
| Second action without cache | 0.9267 s |

### Explicit cache experiment

The DataFrame was cached and materialized using an action.

**Cache materialization runtime:** `1.7301 s`

After materialization:

- `is_cached` returned `True`.
- `explain()` showed `InMemoryTableScan` and `InMemoryRelation`.
- The persisted storage level was displayed as disk + memory with one replica.

The small local workload did not demonstrate a consistent wall-clock speedup from caching. This is expected to be sensitive to Spark/JVM warm-up, local execution overhead, and the very small size of the dataset.

### Interpretation

Caching/persistence is useful when an expensive intermediate result is reused multiple times. It is not beneficial to cache every DataFrame because persistence consumes memory/disk resources and also has an initial materialization cost.

### Evidence

- `13_cache_storage.png`
- `13_cache_physical_plan.png`

---

# 8. Task 5 — `explain(True)` and Catalyst

A deliberately small DataFrame was used to make the Catalyst planning stages easy to interpret.

The query filtered rows where `amount > 500`, calculated a 10% tax column, and selected `account_id` and `tax`.

`explain(True)` displayed four stages:

| Stage | Purpose |
|---|---|
| Parsed Logical Plan | Shows how Spark initially interprets the query. |
| Analyzed Logical Plan | Resolves columns, references, and data types and validates the query. |
| Optimized Logical Plan | Applies Catalyst optimization rules to simplify and improve the logical plan. |
| Physical Plan | Chooses concrete execution operators for running the query. |

## Observed optimization

In the actual output:

- The analyzed plan resolved `account_id` as `string` and `tax` as `double`.
- The optimized plan included an `isnotnull(amount)` condition in addition to `amount > 500`.
- The optimized projection retained only the columns required by the result.
- The physical plan used `Scan ExistingRDD`, `Filter`, and `Project`.

This illustrates the core distinction:

**Logical plan = what Spark needs to do**  
**Physical plan = how Spark will execute it**

Catalyst is Spark SQL's query optimization framework that analyzes logical plans, applies optimization rules, and helps generate the physical execution plan.

### Evidence

- `14_catalyst_explain_true.png`

---

# 9. Overall Performance Results

The most useful measured before/after comparisons are summarized below.

| Optimization | Before | After / Best | Observed result |
|---|---:|---:|---|
| Salting | Baseline aggregation: 3.5668 s | Salted aggregation: 4.1022 s | No wall-clock gain at this small scale; skew distribution improved |
| AQE | 13 s (AQE OFF) | 12 s (AQE ON) | ~1 s stage reduction in this test |
| Broadcast join | 15.3626 s | 13.8781 s | ~9.7% faster |
| Shuffle partitions | 8: 7.0099 s; 200: 5.9353 s | 32: 5.7479 s | 32 was best observed; ~3.2% faster than 200 |
| Cache/persist | No-cache reuse was variable | Explicit persistence verified | No significant speedup demonstrated at this small scale |

These results are intentionally reported as observed measurements rather than as claims about production-scale performance.

---

# 10. Spark UI and DAG Evidence

The Spark UI was used to inspect jobs, stages, task counts, durations, shuffle behavior, and storage state.

The most relevant evidence includes:

- Baseline skew stage: `01_baseline_skew_stage.png`
- Salting distribution: `02_salting_distribution.png`
- Salted aggregation: `03_salted_aggregation_stage.png`
- AQE OFF stage: `04_skewed_join_aqe_off.png`
- AQE ON stage: `05_skewed_join_aqe_on.png`
- Shuffle-join physical plan: `06_shuffle_join_plan.png`
- Broadcast-join physical plan: `08_broadcast_join_plan.png`
- Broadcast stage: `09_broadcast_join_stage.png`
- Cache storage: `13_cache_storage.png`
- Cached physical plan: `13_cache_physical_plan.png`
- Catalyst `explain(True)`: `14_catalyst_explain_true.png`

A Spark DAG represents the stages and dependencies that Spark executes. Shuffle operations typically form stage boundaries because data must be redistributed before downstream processing can continue. The physical plan, in contrast, shows execution operators such as `Filter`, `Project`, `Exchange`, `SortMergeJoin`, and `BroadcastHashJoin`.

---

# 11. Key Lessons

1. **Data skew matters:** A hot key such as A001 can dominate a workload and cause uneven work distribution.
2. **Salting trades complexity for distribution:** It can spread a hot key across buckets, but it introduces an additional aggregation step.
3. **AQE is runtime-aware:** AQE can adapt the execution plan based on runtime statistics and can help with skewed joins.
4. **Broadcast joins can eliminate expensive shuffle-based join work:** They are especially useful for large-fact/small-dimension joins.
5. **Shuffle partition tuning is workload-dependent:** The optimal count should be measured rather than guessed.
6. **Caching is selective:** Persistence is most useful for expensive intermediate data that is reused multiple times.
7. **`explain(True)` is a tuning tool:** The plan reveals how Spark interprets, optimizes, and physically executes a query.

---

# 12. Limitations

This implementation was executed with Spark 4.2.0 using `local[*]` on a personal Windows development machine and a small synthetic dataset of 10,000 transaction records. It does not reproduce the assignment's 50 GB / 4-hour production workload.

Therefore:

- local runtimes are illustrative rather than production forecasts,
- optimization percentages should not be extrapolated directly to a distributed cluster,
- some Spark UI behavior differs from what would be observed on a multi-node production cluster, and
- the experiments demonstrate performance-tuning methodology and Spark execution behavior rather than production capacity planning.

---

# 13. Conclusion

The A9 performance-tuning techniques were implemented and evaluated using a controlled local Spark benchmark.

Data skew was demonstrated with a deliberately hot key and addressed using salting and AQE. A shuffle-based join was replaced with a broadcast join, producing an approximately 9.7% runtime improvement in the local benchmark. Shuffle partition counts of 8, 32, and 200 were compared, with 32 performing best for the tested workload. Persistence was demonstrated and verified through Spark's cached physical plan, while `explain(True)` was used to interpret Catalyst's logical and physical planning stages.

The main performance-engineering lesson is that Spark optimizations should be driven by **execution evidence and representative benchmarks**, not by assumptions that a particular technique or configuration will always be faster.
