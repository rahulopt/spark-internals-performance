# 05 — Spark Interview Questions

## Overview

This document contains commonly asked Spark architecture and execution questions.

The questions are arranged from:

```text
Basic
  ↓
Intermediate
  ↓
Advanced
  ↓
Scenario-Based
```

The goal is not to memorize every answer word-for-word.

Instead, understand the underlying execution model:

```text
Driver
  ↓
Job
  ↓
Stage
  ↓
Task
  ↓
Partition
  ↓
Executor
```

---

# 1. What is Apache Spark?

### Answer

Apache Spark is a distributed data processing engine designed to process large datasets across multiple machines.

It distributes data into partitions and executes computation on those partitions in parallel using executors.

A simplified architecture is:

```text
Driver
   ↓
Cluster Manager
   ↓
Executors
   ↓
Tasks
   ↓
Partitions
```

---

# 2. What is a Spark Application?

A Spark application is a complete program submitted to Spark for execution.

For example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OrdersAnalysis") \
    .getOrCreate()

df = spark.read.parquet("orders.parquet")

df.groupBy("customer_id").count().show()
```

The complete program represents one Spark application.

---

# 3. What are the main components of Spark architecture?

The major components are:

```text
Driver
Cluster Manager
Executors
Worker Nodes
Tasks
Partitions
```

The Driver coordinates the application.

The Cluster Manager allocates resources.

Executors perform the actual computation.

---

# 4. What is the Driver?

The Driver is the coordinator of a Spark application.

Its responsibilities include:

* Creating SparkSession
* Building execution plans
* Creating jobs
* Dividing jobs into stages
* Scheduling tasks
* Tracking execution
* Communicating with executors

Simple mental model:

```text
Driver = Brain of the application
```

---

# 5. What is an Executor?

An executor is a process running on a worker node that executes Spark tasks.

Executors are responsible for:

* Processing partitions
* Running tasks
* Storing cached data
* Storing intermediate data
* Reporting task status

Simple model:

```text
Executor = Worker Process
```

---

# 6. Driver vs Executor?

| Driver                      | Executor                        |
| --------------------------- | ------------------------------- |
| Coordinates application     | Executes tasks                  |
| Builds execution plan       | Processes partitions            |
| Schedules tasks             | Runs tasks                      |
| Tracks execution            | Stores cached/intermediate data |
| Usually one per application | Usually multiple                |

Simple answer:

> The Driver coordinates the work, while Executors perform the work.

---

# 7. What is a Cluster Manager?

A Cluster Manager allocates resources to Spark applications.

Examples:

```text
Standalone
YARN
Kubernetes
```

Its responsibility is primarily resource allocation.

It decides where resources such as CPU and memory can be provided to the application.

---

# 8. What is a Worker Node?

A worker node is a machine in the cluster that provides resources for Spark execution.

Executors run on worker nodes.

```text
Worker Node
    ↓
Executor
    ↓
Tasks
```

---

# 9. Worker Node vs Executor?

They are different.

```text
Worker Node
→ Machine

Executor
→ Process running on that machine
```

For example:

```text
Worker Node 1
└── Executor

Worker Node 2
└── Executor
```

---

# 10. What is SparkSession?

`SparkSession` is the main entry point for modern Spark applications.

Example:

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

It provides access to:

```text
DataFrame API
SQL
SparkContext
Configuration
Catalog
```

---

# 11. What is SparkContext?

`SparkContext` is the lower-level entry point to Spark's core execution functionality.

Modern Spark applications usually create a `SparkSession`, which internally provides access to the underlying Spark context.

Conceptually:

```text
SparkSession
     ↓
SparkContext
     ↓
Spark Cluster
```

---

# 12. What is lazy evaluation?

Lazy evaluation means Spark does not immediately execute transformations.

Example:

```python
df2 = df.filter("amount > 100")
```

Spark records the transformation instead of immediately processing the data.

Execution happens when an action is triggered.

```text
Transformation
      ↓
Lazy
      ↓
Action
      ↓
Execution
```

---

# 13. Why does Spark use lazy evaluation?

Lazy evaluation allows Spark to:

* Build an execution plan
* Optimize the computation
* Combine operations
* Avoid unnecessary computation
* Reduce unnecessary data movement

Instead of executing every operation immediately, Spark can analyze the complete pipeline first.

---

# 14. What are transformations?

Transformations create a new dataset from an existing dataset.

Examples:

```python
filter()
select()
withColumn()
join()
groupBy()
```

Transformations are generally lazy.

---

# 15. What are actions?

Actions trigger execution and produce a result or side effect.

Examples:

```python
show()
count()
collect()
write.parquet()
```

The key relationship:

```text
Transformation
     ↓
Action
     ↓
Job
```

---

# 16. What is a Spark Job?

A Spark Job is a unit of work triggered by an action.

For example:

```python
df.count()
```

can trigger a job.

A Spark application can contain multiple jobs.

```text
Application
│
├── Job 1
├── Job 2
└── Job 3
```

---

# 17. What is a Stage?

A stage is a portion of a Spark job that can be executed together without crossing a shuffle dependency.

A job can contain multiple stages.

```text
Job
│
├── Stage 0
├── Stage 1
└── Stage 2
```

Shuffle boundaries commonly separate stages.

---

# 18. What is a Task?

A task is a unit of execution sent to an executor.

A useful mental model is:

```text
One Task
    ↓
Processes One Partition
```

For example:

```text
Stage
│
├── Task 0 → Partition 0
├── Task 1 → Partition 1
├── Task 2 → Partition 2
└── Task 3 → Partition 3
```

---

# 19. What is a Partition?

A partition is a logical chunk of distributed data.

For example:

```text
Dataset
│
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

Partitions are the basic unit of parallel processing in Spark.

---

# 20. What is the relationship between Job, Stage, Task and Partition?

Remember:

```text
Application
    ↓
Job
    ↓
Stage
    ↓
Task
    ↓
Partition
```

More precisely:

```text
One Application
    ↓
Multiple Jobs

One Job
    ↓
Multiple Stages

One Stage
    ↓
Multiple Tasks

One Task
    ↓
Processes one partition
```

---

# 21. What is a DAG?

DAG stands for:

> Directed Acyclic Graph

Spark represents the sequence and dependencies between transformations as a DAG.

Example:

```text
Read
 ↓
Filter
 ↓
Select
 ↓
GroupBy
 ↓
Aggregate
```

The DAG helps Spark determine how the computation should be executed.

---

# 22. What is a narrow transformation?

A narrow transformation is one where a child partition depends on a relatively limited set of parent partition data.

Examples:

```text
filter
select
withColumn
map
```

Conceptually:

```text
P0 → P0
P1 → P1
P2 → P2
```

No major redistribution is required.

---

# 23. What is a wide transformation?

A wide transformation requires data redistribution between partitions.

Examples:

```text
groupBy
join
distinct
orderBy
repartition
```

Conceptually:

```text
P0 ─┐
P1 ─┼──► Shuffle ──► New Partitions
P2 ─┤
P3 ─┘
```

---

# 24. What is shuffle?

Shuffle is the redistribution of data across partitions.

For example:

```python
df.groupBy("customer_id").count()
```

Records with the same `customer_id` may exist in different partitions.

Spark needs to redistribute them so that records belonging to the same grouping key can be processed together.

---

# 25. Why is shuffle expensive?

Shuffle can involve:

```text
Network I/O
Disk I/O
Serialization
Memory
CPU
```

Data may need to move between executors.

```text
Executor 1 ─┐
Executor 2 ─┼──► Network ──► Executors
Executor 3 ─┘
```

Therefore, excessive shuffle can hurt performance.

---

# 26. What causes shuffle?

Common operations that may cause shuffle include:

```text
groupBy
join
distinct
orderBy
repartition
reduceByKey
```

However, the exact physical execution depends on the query plan and optimization strategy.

For example, some joins can use broadcast joins and avoid a large shuffle.

---

# 27. Does every join cause shuffle?

No.

A join can be executed using different strategies.

For example:

```text
SortMergeJoin
BroadcastHashJoin
```

A broadcast join can avoid shuffling the large table when the smaller side is broadcast.

Therefore:

> Do not assume every join automatically means a full shuffle of both datasets.

Always inspect the physical plan when performance matters.

---

# 28. What is a stage boundary?

A stage boundary occurs where Spark cannot continue the same pipelined execution because of a dependency such as a shuffle.

Example:

```text
Stage 0
   ↓
Shuffle
   ↓
Stage 1
```

A shuffle commonly creates a stage boundary.

---

# 29. What is `Exchange` in a Spark physical plan?

`Exchange` generally represents a redistribution of data between partitions.

For example:

```text
Filter
  ↓
Exchange
  ↓
HashAggregate
```

Think:

```text
Exchange
   ↓
Shuffle / Redistribution
```

`Exchange` is therefore an important clue when analyzing Spark performance.

---

# 30. How many tasks are created for a stage?

A useful rule is:

```text
Tasks
≈
Partitions processed by the stage
```

For example, if a stage processes 100 partitions:

```text
≈ 100 tasks
```

These tasks may not all run simultaneously.

Their execution depends on available task slots.

---

# 31. Can all tasks run at the same time?

Not necessarily.

Suppose:

```text
100 partitions
```

but only:

```text
20 available task slots
```

Spark may process them in waves:

```text
20
20
20
20
20
```

So:

```text
Parallelism
depends on
Available Resources
+
Number of Partitions
```

---

# 32. What is executor parallelism?

Suppose we have:

```text
3 executors
4 cores per executor
```

A simplified estimate of concurrent task capacity is:

```text
3 × 4 = 12 tasks
```

assuming the configuration allows those cores to be used for task execution.

---

# 33. What happens if there are too few partitions?

Suppose:

```text
4 partitions
100 task slots
```

Only a small number of tasks can run.

Many resources remain unused.

```text
100 task slots
     ↑
4 tasks
```

This can cause poor cluster utilization.

---

# 34. What happens if there are too many partitions?

Suppose a dataset is divided into millions of tiny partitions.

Spark has to schedule a very large number of tasks.

This can create:

```text
Task scheduling overhead
Task startup overhead
Metadata overhead
```

Therefore:

```text
Too Few Partitions
→ Poor Parallelism

Too Many Tiny Partitions
→ Scheduling Overhead
```

---

# 35. What is data skew?

Data skew occurs when data is unevenly distributed between partitions.

Example:

```text
Partition 0 → 10 MB
Partition 1 → 12 MB
Partition 2 → 11 MB
Partition 3 → 2 GB
```

One task becomes much slower:

```text
Task 0 → 2 sec
Task 1 → 2 sec
Task 2 → 3 sec
Task 3 → 180 sec
```

The entire stage may have to wait for the slow task.

---

# 36. What is a straggler task?

A straggler task is a task that takes significantly longer than other tasks in the same stage.

Example:

```text
Task 1 → 2 sec
Task 2 → 3 sec
Task 3 → 2 sec
Task 4 → 150 sec
```

Possible causes include:

```text
Data skew
Large partition
Slow I/O
GC pressure
Executor problems
```

---

# 37. How do you identify data skew?

Use:

```text
Spark UI
```

Look at:

```text
Task duration
Input size
Shuffle read
Shuffle write
```

If one or a few tasks process significantly more data than the others, data skew may be present.

---

# 38. What is a broadcast join?

A broadcast join sends a small dataset to executors so that the large dataset does not need to be shuffled in the same way.

Conceptually:

```text
Small Table
    ↓
Broadcast
    ↓
All Executors
    ↓
Join with Large Table
```

Example:

```python
from pyspark.sql.functions import broadcast

result = large_df.join(
    broadcast(small_df),
    "customer_id"
)
```

Broadcast joins can be very effective when one side is sufficiently small.

---

# 39. What is a sort-merge join?

A sort-merge join is a common distributed join strategy.

Conceptually:

```text
Left Data
   ↓
Shuffle
   ↓
Sort

Right Data
   ↓
Shuffle
   ↓
Sort

      ↓
    Merge
```

It is useful for large datasets where broadcasting is not practical.

---

# 40. What is caching?

Caching stores computed data so it can be reused.

Example:

```python
df.cache()
```

Suppose:

```python
df.cache()

df.count()
df.groupBy("category").count()
df.filter("amount > 100").count()
```

The cached data may reduce repeated computation.

---

# 41. When should you cache?

Cache when:

```text
The same DataFrame is reused
multiple times
```

For example:

```text
DataFrame
   ├── Action 1
   ├── Action 2
   └── Action 3
```

Caching may prevent Spark from recomputing the same upstream data for each action.

Do not cache data that is used only once.

---

# 42. What is persistence?

`persist()` allows you to choose a storage level.

Example:

```python
df.persist()
```

Compared with:

```python
df.cache()
```

`cache()` uses the default storage level for that API, while `persist()` allows explicit control over the storage level.

---

# 43. What happens if cached data does not fit?

Depending on the selected storage level, Spark may use disk or recompute partitions when cached data is lost or cannot be retained.

The important idea is:

```text
Cache
≠
Permanent Storage
```

Caching is an optimization, not a replacement for reliable source data.

---

# 44. What is lineage?

Lineage represents the sequence of transformations used to produce a dataset.

Example:

```text
Source
  ↓
Filter
  ↓
Select
  ↓
GroupBy
```

If intermediate data is lost, Spark can use lineage to recompute the required data.

This contributes to Spark's fault tolerance.

---

# 45. How does Spark handle task failure?

If a task fails, Spark can retry the task.

Conceptually:

```text
Task
 ↓
Failure
 ↓
Retry
 ↓
Another attempt
```

If the task repeatedly fails, the stage or application may eventually fail depending on the circumstances and retry configuration.

---

# 46. What is fault tolerance in Spark?

Spark provides fault tolerance through mechanisms such as:

```text
Lineage
Task retries
Recomputation
```

If an intermediate partition is lost, Spark can often recompute it rather than requiring the entire application to restart.

---

# 47. What is client mode?

In client mode:

```text
Driver
   ↓
Runs on submission machine
```

Example:

```text
Laptop / EMR Primary Node
        ↓
      Driver
        ↓
    Executors
```

Useful for:

```text
Interactive development
Debugging
Testing
```

---

# 48. What is cluster mode?

In cluster mode:

```text
Driver
   ↓
Runs inside cluster
```

Architecture:

```text
Client
  │
  │ Submit
  ▼
Cluster
├── Driver
├── Executor
├── Executor
└── Executor
```

It is commonly preferred for production and automated workloads.

---

# 49. Client Mode vs Cluster Mode?

| Client Mode                           | Cluster Mode                              |
| ------------------------------------- | ----------------------------------------- |
| Driver on submission machine          | Driver inside cluster                     |
| Useful for interactive work           | Useful for production                     |
| Client environment hosts Driver       | Cluster hosts Driver                      |
| More dependent on client connectivity | Less dependent on client after submission |

The one thing to remember:

> **Deploy mode primarily determines where the Driver runs.**

---

# 50. What is the difference between `--master` and `--deploy-mode`?

### `--master`

Defines the cluster manager / execution environment.

Example:

```bash
--master yarn
```

### `--deploy-mode`

Defines where the Driver runs.

Example:

```bash
--deploy-mode cluster
```

Therefore:

```text
--master
→ Resource management environment

--deploy-mode
→ Driver location
```

---

# 51. Why can the Driver become a bottleneck?

The Driver coordinates the application.

If we send huge amounts of data to it:

```python
df.collect()
```

or:

```python
df.toPandas()
```

the Driver may run out of memory.

Architecture:

```text
Executors
   │
   │ Huge Result
   ▼
Driver
   │
   ▼
Memory Pressure
```

---

# 52. Why is `collect()` dangerous?

`collect()` brings all records to the Driver.

For example:

```python
data = df.collect()
```

If the DataFrame contains millions of rows, the Driver may not have enough memory.

Better approaches include:

```python
df.show()
df.limit(100).collect()
```

when you only need a small sample.

---

# 53. Why can `toPandas()` be dangerous?

`toPandas()` also brings the distributed dataset to the Driver.

```python
pdf = df.toPandas()
```

Therefore:

```text
Spark DataFrame
      ↓
Driver
      ↓
Pandas DataFrame
```

This should only be used when the result is small enough for Driver memory.

---

# 54. What is Spark UI used for?

Spark UI helps analyze application execution.

Important tabs:

```text
Jobs
Stages
Storage
Environment
Executors
SQL
```

You can investigate:

```text
Slow stages
Task failures
Shuffle
Data skew
Executor usage
Memory
Spill
```

---

# 55. How would you debug a slow Spark job?

I would follow this sequence:

```text
1. Check Spark UI
       ↓
2. Identify slow stage
       ↓
3. Check task distribution
       ↓
4. Check data skew
       ↓
5. Check shuffle read/write
       ↓
6. Check partition count
       ↓
7. Check physical plan
       ↓
8. Check join strategy
       ↓
9. Check executor resource usage
       ↓
10. Optimize based on the bottleneck
```

Do not blindly increase cluster size.

---

# 56. How would you optimize a shuffle-heavy job?

First identify why the shuffle is happening.

Then consider:

```text
Reduce unnecessary columns
Filter early
Use appropriate partitioning
Avoid unnecessary repartition()
Use broadcast join when appropriate
Handle data skew
Avoid unnecessary distinct/orderBy
```

The exact optimization depends on the workload.

---

# 57. Why should we filter data early?

Suppose we have:

```text
1 TB Input
```

and only:

```text
100 GB
```

is actually required.

Filtering early can reduce the amount of data that flows through later operations.

Conceptually:

```text
Bad:

1 TB
 ↓
Shuffle
 ↓
Filter
```

Better:

```text
1 TB
 ↓
Filter
 ↓
100 GB
 ↓
Shuffle
```

This can significantly reduce shuffle and network traffic.

---

# 58. What is predicate pushdown?

Predicate pushdown means pushing filters closer to the data source when supported.

Example:

```python
df.filter("age > 30")
```

Instead of reading and processing everything first, Spark and the underlying data source may be able to filter data earlier.

Benefits:

```text
Less data read
Less network traffic
Less computation
```

---

# 59. What is column pruning?

Column pruning means reading only the columns required by the query when the source supports it.

Suppose a dataset contains:

```text
100 columns
```

but the query only needs:

```text
customer_id
revenue
```

Spark can often avoid reading unnecessary columns.

This is particularly beneficial with columnar formats such as Parquet.

---

# 60. Why is Parquet commonly used with Spark?

Parquet is a columnar storage format.

Advantages include:

```text
Column pruning
Predicate pushdown support
Compression
Efficient analytical reads
```

For analytical Spark workloads, Parquet is often much more efficient than row-oriented formats.

---

# 61. What is spill?

When Spark cannot keep required intermediate data entirely in memory, it may spill data to disk.

Conceptually:

```text
Memory
  ↓
Not Enough
  ↓
Disk Spill
```

Spilling can slow down execution because disk I/O is slower than memory.

Spark UI can help identify spilling.

---

# 62. What is an executor out-of-memory error?

An executor OOM occurs when an executor cannot allocate enough memory for its workload.

Possible causes include:

```text
Large partitions
Data skew
Large joins
Caching too much data
Insufficient executor memory
Memory-intensive operations
```

Do not automatically assume:

```text
More memory = solution
```

First identify the underlying cause.

---

# 63. What is a Driver OOM?

Driver OOM occurs when the Driver runs out of memory.

Common causes:

```python
df.collect()
df.toPandas()
```

or creating very large driver-side objects.

The solution is often to keep computation distributed instead of bringing the data to the Driver.

---

# 64. What is the difference between Driver OOM and Executor OOM?

### Driver OOM

Usually related to:

```text
Driver-side data
Large collected results
Application metadata
Large driver objects
```

### Executor OOM

Usually related to:

```text
Large partitions
Shuffle
Caching
Joins
Data skew
Task execution
```

---

# 65. What happens when an executor fails?

Spark can detect executor/task failures and may reschedule affected tasks.

If cached data is lost, Spark may recompute the lost partitions from lineage.

Conceptually:

```text
Executor Failed
      ↓
Lost Partition
      ↓
Recompute
      ↓
New Executor
      ↓
Continue
```

The exact recovery behavior depends on the type of failure and stage.

---

# 66. What happens when the Driver fails?

The Driver coordinates the Spark application.

Therefore, a Driver failure is much more serious than a single task failure.

In general:

```text
Executor Failure
→ Tasks may be retried

Driver Failure
→ Application generally fails
```

This is why Driver reliability is important.

---

# 67. Why does Spark use partitions?

Partitions allow Spark to distribute work across multiple executors.

Instead of:

```text
Entire Dataset
```

Spark works with:

```text
Partition 0
Partition 1
Partition 2
Partition 3
```

which can be processed concurrently.

---

# 68. What determines parallelism in Spark?

Important factors include:

```text
Number of partitions
Executor cores
Number of executors
Cluster resources
Task scheduling
```

A simple mental model:

```text
Parallelism
≈
Number of runnable partitions
vs
Available task slots
```

---

# 69. What is repartition?

`repartition()` changes the number or distribution of partitions.

Example:

```python
df.repartition(20)
```

This generally causes a shuffle because data is redistributed.

Use it when you intentionally need to rebalance or redistribute data.

---

# 70. What is coalesce?

`coalesce()` is commonly used to reduce the number of partitions.

Example:

```python
df.coalesce(5)
```

It can often reduce partitions without a full shuffle.

However, reducing partitions too aggressively can create large partitions and reduce parallelism.

---

# 71. Repartition vs Coalesce

| `repartition()`                  | `coalesce()`                   |
| -------------------------------- | ------------------------------ |
| Can increase/decrease partitions | Primarily decreases partitions |
| Usually involves shuffle         | Can avoid full shuffle         |
| Better for redistribution        | Useful for reducing partitions |
| More expensive                   | Often cheaper                  |

Example:

```python
df.repartition(20)
```

versus:

```python
df.coalesce(5)
```

---

# 72. What is `explain()`?

`explain()` shows Spark's execution plan.

Example:

```python
df.explain("formatted")
```

It helps identify:

```text
Scan
Filter
Exchange
Join
Aggregate
Sort
```

This is one of the first tools to use when investigating query performance.

---

# 73. Scenario: One task is much slower than others. What do you check?

I would check:

```text
1. Data skew
2. Partition size
3. Shuffle read
4. Shuffle write
5. Executor health
6. Garbage collection
7. Input data distribution
```

If one partition is much larger than others, skew is a strong possibility.

---

# 74. Scenario: Spark job has huge shuffle. What do you check?

Check:

```text
GroupBy
Join
Distinct
OrderBy
Repartition
```

Then inspect:

```python
df.explain("formatted")
```

Also check Spark UI for:

```text
Shuffle Read
Shuffle Write
Stage duration
Task distribution
```

---

# 75. Scenario: Driver is running out of memory. What do you check?

First look for:

```python
collect()
toPandas()
```

Also check:

```text
Large driver-side objects
Huge query results
Driver configuration
Application metadata
```

The first question should be:

> Why is so much data being brought to the Driver?

---

# 76. Scenario: Executors are idle. What could be wrong?

Possible reasons:

```text
Too few partitions
Small dataset
Insufficient parallelism
Stage dependency
Waiting for shuffle
Data skew
```

For example:

```text
100 executors
4 partitions
```

does not mean all executors will be busy.

There are only 4 partitions to process.

---

# 77. Scenario: More executors did not improve performance. Why?

Possible reasons:

```text
Too few partitions
Data skew
Shuffle bottleneck
I/O bottleneck
Driver bottleneck
Small dataset
Poor join strategy
```

Adding resources does not fix every bottleneck.

---

# 78. Scenario: One executor is doing much more work.

Possible causes:

```text
Data skew
Uneven partitioning
Skewed join key
Large partition
```

Check Spark UI for task-level input and shuffle metrics.

---

# 79. Scenario: Join is very slow. What do you check?

I would check:

```text
1. Join keys
2. Data size
3. Physical join strategy
4. Broadcast possibility
5. Shuffle size
6. Data skew
7. Partitioning
8. Input filtering
```

Use:

```python
df.explain("formatted")
```

to inspect the physical plan.

---

# 80. Scenario: Small table + huge table join

Suppose:

```text
orders = 500 GB
customers = 20 MB
```

A broadcast join may be a good candidate:

```python
from pyspark.sql.functions import broadcast

result = orders.join(
    broadcast(customers),
    "customer_id"
)
```

But always ensure the smaller dataset is genuinely safe to broadcast given the cluster's memory and configuration.

---

# 81. Scenario: Large table + large table join

Suppose:

```text
orders = 500 GB
transactions = 700 GB
```

Broadcasting either table may be inappropriate.

Spark may use a distributed join such as:

```text
SortMergeJoin
```

depending on the physical plan and configuration.

The focus should be on:

```text
Partitioning
Shuffle
Data Skew
Filtering
Join Keys
```

---

# 82. Scenario: Application works locally but fails on cluster

Possible causes include:

```text
Different Spark version
Different Python version
Missing dependencies
Missing JARs
Wrong file paths
Permission issues
Resource limitations
Environment variables
AWS/S3 permissions
```

A cluster environment is not necessarily identical to your local environment.

---

# 83. Scenario: Application is slow only in production

Check differences between environments:

```text
Input size
Partition count
Data distribution
Data skew
Cluster resources
Spark configuration
Join strategy
Storage location
Network
Caching
```

Production data may behave very differently from development data.

---

# 84. Scenario: How would you explain a Spark job in an interview?

A strong explanation:

> When a Spark application starts, the Driver coordinates the application and requests resources through the cluster manager. Executors are launched on worker nodes. Transformations are evaluated lazily, so Spark builds an execution plan instead of immediately processing the data. When an action is triggered, Spark creates a job. The job is divided into stages based largely on dependency and shuffle boundaries. Each stage is divided into tasks, and each task processes a partition. Executors run these tasks in parallel and report their status back to the Driver.

---

# 85. Advanced Interview Question: Why does shuffle create a stage boundary?

Because after a shuffle, downstream tasks cannot proceed until the required upstream partitioned data is available.

Conceptually:

```text
Stage 0
   ↓
Shuffle
   ↓
Stage 1
```

The dependency between the two stages is not simply a one-to-one partition dependency.

Data must be redistributed before the next stage can process it.

---

# 86. Advanced Interview Question: Why are narrow transformations faster?

They generally avoid large-scale data redistribution.

Example:

```text
Filter
```

can process each partition independently.

Whereas:

```text
GroupBy
```

requires records with the same key to be brought together.

Therefore:

```text
Narrow
→ Less network movement

Wide
→ More data movement
```

---

# 87. Advanced Interview Question: Does a wide transformation always mean one shuffle?

Not necessarily.

The exact execution depends on the physical plan.

A query may contain multiple shuffle operations.

For example:

```text
Join
  ↓
Shuffle
  ↓
Aggregation
  ↓
Shuffle
```

Therefore, inspect the physical plan and Spark UI rather than assuming the exact number of shuffles from the source code alone.

---

# 88. Advanced Interview Question: What is the difference between logical and physical plan?

### Logical Plan

Describes:

```text
What should be computed?
```

### Physical Plan

Describes:

```text
How should Spark compute it?
```

Example:

```text
Logical:
Filter → Join → Aggregate

Physical:
FileScan → Filter → Exchange → SortMergeJoin → HashAggregate
```

---

# 89. Advanced Interview Question: What is Catalyst?

Catalyst is Spark SQL's query optimization framework.

It analyzes logical plans and applies optimizations before generating a physical execution plan.

Examples:

```text
Predicate Pushdown
Column Pruning
Constant Folding
Expression Simplification
Join Optimizations
```

---

# 90. Advanced Interview Question: What is Tungsten?

Tungsten refers to Spark's project focused on improving execution efficiency through better memory and CPU utilization.

Key ideas historically associated with Tungsten include:

```text
Efficient memory management
Binary processing
Whole-stage code generation
CPU-efficient execution
```

For modern Spark interviews, focus more on practical execution concepts such as:

```text
Memory
Shuffle
Code Generation
Physical Plans
Execution Performance
```

---

# 91. Advanced Interview Question: What is whole-stage code generation?

Spark can generate optimized code that combines multiple operators into a more efficient execution pipeline.

Conceptually:

```text
Filter
  ↓
Project
  ↓
Expression
```

can be optimized into a more efficient generated execution path.

This reduces some interpretation and function-call overhead.

---

# 92. Advanced Interview Question: Why doesn't Spark simply use all cluster memory?

Because memory is divided across different responsibilities.

Resources may be needed for:

```text
Execution
Storage
Shuffle
Application overhead
Operating system
Other processes
```

The exact memory model depends on Spark version and deployment configuration.

---

# 93. Advanced Interview Question: What is spill-to-disk?

When Spark needs more memory than is available for certain operations, intermediate data may be spilled to disk.

Conceptually:

```text
Memory
  ↓
Insufficient
  ↓
Disk
```

Disk I/O is generally slower than memory, so excessive spilling can hurt performance.

---

# 94. Advanced Interview Question: What is speculative execution?

Speculative execution allows Spark to launch another attempt of a slow-running task when Spark suspects that the original task is unusually slow.

Conceptually:

```text
Task 1 → Slow
     ↓
Speculative Attempt
     ↓
Another Executor
```

This can help with straggler tasks caused by temporary executor or infrastructure problems.

However, speculative execution does not solve underlying data skew.

---

# 95. Advanced Interview Question: What is dynamic allocation?

Dynamic allocation allows Spark to adjust the number of executors based on workload.

Conceptually:

```text
Low Workload
    ↓
Fewer Executors

High Workload
    ↓
More Executors
```

This can improve resource utilization in suitable environments.

---

# 96. Advanced Interview Question: What is executor memory overhead?

Executor memory is not limited to the JVM heap alone.

Additional memory can be required for things such as:

```text
Off-heap usage
Native processes
Python workers
JVM overhead
Other container-related usage
```

Therefore, Spark has memory-overhead configuration in cluster deployments.

This is especially important for PySpark workloads.

---

# 97. Advanced Interview Question: Why can PySpark have additional memory pressure?

PySpark involves communication between the JVM-based Spark engine and Python processes.

Conceptually:

```text
JVM Executor
      ↕
Python Worker
```

Python workloads can therefore have memory requirements beyond the JVM heap.

When debugging PySpark memory issues, consider both Spark executor memory and Python/overhead requirements.

---

# 98. Advanced Interview Question: Why is `toPandas()` dangerous?

Because the distributed Spark DataFrame is converted into a local Pandas DataFrame.

```text
Executors
    ↓
Driver
    ↓
Pandas
```

The entire result must fit within the available Driver-side resources.

Therefore:

> `toPandas()` should only be used when the result is small enough.

---

# 99. Advanced Interview Question: How do you optimize a Spark application?

A practical optimization workflow:

```text
1. Understand the data
        ↓
2. Inspect Spark UI
        ↓
3. Inspect physical plan
        ↓
4. Identify bottleneck
        ↓
5. Reduce unnecessary data
        ↓
6. Optimize joins
        ↓
7. Optimize partitioning
        ↓
8. Handle skew
        ↓
9. Tune resources if necessary
        ↓
10. Measure again
```

Optimization should be evidence-driven.

---

# 100. Most Important Questions to Memorize

If you have limited interview preparation time, focus on these:

```text
1. What is Spark?
2. Explain Spark architecture.
3. Driver vs Executor.
4. What is Cluster Manager?
5. What is SparkSession?
6. What is lazy evaluation?
7. Transformation vs Action.
8. What is a Job?
9. What is a Stage?
10. What is a Task?
11. What is a Partition?
12. Explain Job → Stage → Task → Partition.
13. Narrow vs Wide transformation.
14. What is Shuffle?
15. Why is Shuffle expensive?
16. What causes stage boundaries?
17. Client vs Cluster mode.
18. --master vs --deploy-mode.
19. What is data skew?
20. How do you debug a slow Spark job?
21. Broadcast Join vs Sort-Merge Join.
22. Driver OOM vs Executor OOM.
23. How does Spark achieve fault tolerance?
24. What is caching?
25. How do you optimize a Spark application?
```

---

# 101. The Ultimate Spark Interview Diagram

Before answering any architecture question, visualize this:

```text
                         APPLICATION
                              │
                              ▼
                           DRIVER
                              │
               ┌──────────────┴──────────────┐
               │                             │
        Execution Plan                 Scheduling
               │                             │
               └──────────────┬──────────────┘
                              │
                             JOB
                              │
                            STAGES
                              │
                       ┌──────┴──────┐
                       │             │
                    TASKS         TASKS
                       │             │
                  PARTITIONS    PARTITIONS
                       │             │
                       └──────┬──────┘
                              │
                         EXECUTORS
                              │
                    Distributed Processing
```

And remember:

```text
Driver
→ Coordinates

Cluster Manager
→ Allocates Resources

Executor
→ Executes Tasks

Task
→ Processes Partition

Shuffle
→ Redistributes Data

Stage
→ Boundary around Dependencies

Action
→ Triggers Job
```

---

# 102. Final Interview Strategy

When an interviewer asks a Spark question, don't jump directly into configuration.

Start with the execution model.

For example, if asked:

> "Why is this Spark job slow?"

Think:

```text
Job
 ↓
Stages
 ↓
Tasks
 ↓
Partitions
 ↓
Shuffle
 ↓
Skew
 ↓
Resources
```

Then inspect the evidence:

```text
Spark UI
+
Physical Plan
+
Task Metrics
```

This approach makes your answer much stronger than simply saying:

> "Increase executor memory."

---

# Final Takeaway

The most important Spark concepts are connected:

```text
Spark Application
       ↓
     Driver
       ↓
   Lazy Plan
       ↓
     Action
       ↓
      Job
       ↓
    Stages
       ↓
     Tasks
       ↓
   Partitions
       ↓
   Executors
       ↓
Distributed Processing
```

And the performance story is:

```text
Partitions
    ↓
Parallelism
    ↓
Tasks
    ↓
Executors

Wide Dependencies
    ↓
Shuffle
    ↓
Network / Disk / CPU
    ↓
Potential Bottleneck

Data Skew
    ↓
Uneven Partitions
    ↓
Straggler Tasks
    ↓
Slow Stage
```

> **If you understand Driver → Job → Stage → Task → Partition → Executor → Shuffle, you understand the foundation of Spark execution and performance.**
