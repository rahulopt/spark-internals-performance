# 03 — Memory Management: Practical Guide

## Overview

In the previous two lessons, we learned the Spark memory model.

Now we will apply those concepts to real Spark applications.

The goal of this lesson is not to memorize configuration values.

The goal is to learn how to answer:

> **"My Spark application is slow or running out of memory. What should I check first?"**

A practical Spark memory troubleshooting process looks like this:

```text
Spark Application
       │
       ▼
Identify Problem
       │
       ├── Driver OOM
       ├── Executor OOM
       ├── Container killed
       ├── Excessive GC
       ├── Excessive spill
       └── Skew
       │
       ▼
Inspect Spark UI
       │
       ▼
Identify Root Cause
       │
       ▼
Tune Code / Partitions / Configuration
       │
       ▼
Run Again
       │
       ▼
Compare Metrics
```

---

# 1. Start With the Problem, Not the Configuration

A common mistake is:

```text
Application is slow
        ↓
Increase executor memory
        ↓
Run again
```

This is not a good tuning strategy.

Instead:

```text
Application is slow
        ↓
Find the slow stage
        ↓
Inspect task metrics
        ↓
Check shuffle / spill / GC / skew
        ↓
Identify root cause
        ↓
Apply targeted optimization
```

Memory tuning should be **evidence-driven**.

---

# 2. Common Spark Memory Problems

There are several different problems that can look like "Spark is running out of memory."

### Problem 1 — Driver OOM

```text
java.lang.OutOfMemoryError: Java heap space
```

Potential causes:

* Large `collect()`
* Large `toPandas()`
* Huge driver-side metadata
* Large result returned to driver

---

### Problem 2 — Executor OOM

```text
java.lang.OutOfMemoryError: Java heap space
```

Potential causes:

* Large aggregation
* Large join
* Large broadcast
* Huge partition
* Too much cached data
* Data skew

---

### Problem 3 — Container Memory Error

Example:

```text
Container killed for exceeding memory limits
```

Potential causes:

* Insufficient memory overhead
* Python worker memory
* Native memory
* Off-heap memory
* Total process memory exceeding container allocation

---

### Problem 4 — Excessive Garbage Collection

The application may not fail, but tasks become very slow.

```text
High GC Time
     ↓
CPU spent cleaning objects
     ↓
Less CPU for computation
     ↓
Slow tasks
```

---

### Problem 5 — Excessive Spill

```text
Memory
  ↓
Insufficient
  ↓
Spill to Disk
  ↓
Large Disk I/O
  ↓
Slow Stage
```

---

# 3. Practical Configuration

A typical Spark submission may look like:

```bash
spark-submit \
  --master yarn \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 8g \
  --driver-memory 4g \
  application.py
```

Here:

```text
num-executors = 4
executor-cores = 4
executor-memory = 8 GB
driver-memory = 4 GB
```

Conceptually:

```text
4 Executors
│
├── Executor 1 → 4 cores + 8 GB
├── Executor 2 → 4 cores + 8 GB
├── Executor 3 → 4 cores + 8 GB
└── Executor 4 → 4 cores + 8 GB
```

Total executor JVM heap:

```text
4 × 8 GB = 32 GB
```

---

# 4. Add Memory Overhead

For workloads that need additional container memory:

```bash
spark-submit \
  --num-executors 4 \
  --executor-cores 4 \
  --executor-memory 8g \
  --conf spark.executor.memoryOverhead=2048 \
  application.py
```

Conceptually:

```text
Executor Container
│
├── JVM Heap
│   └── 8 GB
│
└── Memory Overhead
    └── 2 GB
```

Total container allocation is therefore approximately:

```text
8 GB + 2 GB = 10 GB
```

per executor, subject to the deployment environment and other resource accounting.

---

# 5. PySpark Example

Suppose we run:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MemoryExample") \
    .getOrCreate()

df = spark.read.parquet("s3://bucket/orders/")

result = (
    df.groupBy("customer_id")
      .sum("revenue")
)

result.show()
```

Potential memory consumers include:

```text
Executor
│
├── JVM
│   ├── Spark execution
│   ├── Shuffle
│   └── Aggregation
│
└── Python Worker
```

If the workload is Python-heavy, memory overhead becomes more important.

---

# 6. Practical Example: Driver OOM

Consider:

```python
df = spark.read.parquet("s3://bucket/large_dataset/")

data = df.collect()
```

This is dangerous for large datasets.

Architecture:

```text
Executor 1 ─┐
Executor 2 ─┤
Executor 3 ─┼──────► Driver
Executor 4 ─┤
Executor 5 ─┘
```

The driver must hold the collected result.

If the result is too large:

```text
Driver Memory
      ↓
Full
      ↓
OutOfMemoryError
```

### Better approach

Keep the processing distributed:

```python
df.groupBy("category").count().show()
```

or write the result to storage:

```python
result.write.mode("overwrite").parquet(
    "s3://bucket/output/"
)
```

---

# 7. Practical Example: `toPandas()`

This is another common source of driver memory problems.

```python
pdf = df.toPandas()
```

Conceptually:

```text
Spark DataFrame
      │
      ▼
Collect data
      │
      ▼
Driver
      │
      ▼
Pandas DataFrame
```

If the Spark DataFrame is large:

```text
Driver Memory
     ↓
OOM
```

Use `toPandas()` only when the resulting dataset is reasonably small.

---

# 8. Practical Example: Excessive Cache

Suppose:

```python
df1.cache()
df2.cache()
df3.cache()
df4.cache()
```

and each DataFrame is large.

You may see:

```text
Storage Memory
│
├── df1
├── df2
├── df3
└── df4
       ↓
Memory Pressure
```

If these DataFrames are not reused, caching them provides little benefit.

### Better approach

Cache only when:

```text
Data is reused
        AND
Recomputation is expensive
```

---

# 9. Always Materialize the Cache

Calling:

```python
df.cache()
```

does not immediately load the complete DataFrame into memory.

Spark is lazy.

For example:

```python
df.cache()

df.count()
```

The action:

```python
count()
```

causes Spark to compute the DataFrame and populate the cache.

Conceptually:

```text
cache()
   │
   │ Transformation
   ▼
No immediate computation

count()
   │
   ▼
Job executes
   │
   ▼
Partitions cached
```

---

# 10. Unpersist Unused Data

If a cached DataFrame is no longer needed:

```python
df.unpersist()
```

This tells Spark that the cached blocks can be removed.

Example:

```python
df.cache()

df.count()

# Use df

df.unpersist()
```

This can free storage memory for later stages.

---

# 11. Practical Example: Large Join

Consider:

```python
result = orders_df.join(
    customers_df,
    "customer_id"
)
```

A join may require:

```text
Shuffle
+
Execution Memory
+
Network I/O
+
Disk Spill
```

If both datasets are large:

```text
orders
   │
   ├── Shuffle
   │
   ▼
Partitioned Data

customers
   │
   ├── Shuffle
   │
   ▼
Partitioned Data
```

Large shuffles can create significant memory pressure.

---

# 12. Broadcast Join

If one side of the join is small enough, Spark may use a broadcast join.

Example:

```python
from pyspark.sql import functions as F

result = orders_df.join(
    F.broadcast(customers_df),
    "customer_id"
)
```

Conceptually:

```text
Small customers_df
        │
        ▼
     Broadcast
        │
   ┌────┼────┬────┐
   ▼    ▼    ▼    ▼
 Exec1 Exec2 Exec3 Exec4
   │    │    │    │
   └────┴────┴────┘
          │
       orders_df
```

This can avoid a large shuffle of the small dataset.

But:

> Never broadcast a dataset just because you want to avoid shuffle.

The broadcast side must be small enough to fit comfortably within executor resources.

---

# 13. Practical Example: Data Skew

Suppose we have:

```text
customer_id = 1001 → 50 million rows
customer_id = 1002 → 10 rows
customer_id = 1003 → 20 rows
```

When partitioned by `customer_id`:

```text
Partition 1 → 50 million rows
Partition 2 → 10 rows
Partition 3 → 20 rows
```

One task becomes much larger than the others.

Result:

```text
Task 1 → 30 minutes
Task 2 → 10 seconds
Task 3 → 8 seconds
```

This is a classic sign of data skew.

---

# 14. How Skew Creates Memory Problems

Suppose:

```text
Partition 1 → 80 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 1 GB
```

The executor processing Partition 1 may run out of memory.

Meanwhile:

```text
Executor 2 → mostly idle
Executor 3 → mostly idle
Executor 4 → mostly idle
```

So:

> Total cluster memory can be sufficient while one executor still runs out of memory.

---

# 15. Partition Count and Memory

Partitioning directly affects memory usage per task.

Suppose:

```text
100 GB Data
```

with:

```text
10 partitions
```

Average partition size:

```text
100 GB / 10
= 10 GB
```

Now use:

```text
1000 partitions
```

Average partition size:

```text
100 GB / 1000
= 100 MB
```

Smaller partitions can reduce per-task memory requirements.

However, too many partitions create scheduling and task overhead.

Therefore:

```text
Too Few Partitions
        ↓
Large Tasks
        ↓
Memory Pressure

Too Many Partitions
        ↓
Too Many Tasks
        ↓
Scheduling Overhead
```

The goal is a reasonable partition size and good parallelism.

---

# 16. `spark.sql.shuffle.partitions`

For Spark SQL/DataFrame operations, a very important configuration is:

```properties
spark.sql.shuffle.partitions
```

It controls the default number of partitions used for many shuffle operations.

Example:

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    400
)
```

Suppose:

```text
100 GB
```

and:

```text
spark.sql.shuffle.partitions = 10
```

This may create very large shuffle partitions.

Increasing the partition count can reduce the amount of data processed by each task.

But setting it extremely high can create many tiny tasks.

---

# 17. Adaptive Query Execution

Modern Spark provides:

```text
Adaptive Query Execution
(AQE)
```

AQE can dynamically adjust parts of the physical execution plan based on runtime statistics.

Enable it with:

```python
spark.conf.set(
    "spark.sql.adaptive.enabled",
    "true"
)
```

AQE can help with problems such as:

* Too many shuffle partitions
* Small shuffle partitions
* Some forms of data skew
* Join strategy adaptation

This makes Spark more adaptive during execution instead of relying entirely on static planning.

---

# 18. Memory and Executor Cores

Executor memory should also be considered together with executor cores.

Suppose:

```text
Executor
├── 8 cores
└── 8 GB memory
```

Multiple tasks may run concurrently.

Conceptually:

```text
Executor
│
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
├── Core 4 → Task
├── Core 5 → Task
├── Core 6 → Task
├── Core 7 → Task
└── Core 8 → Task
```

If many concurrent tasks require memory, memory pressure can increase.

Therefore:

> Executor cores and executor memory should be considered together.

---

# 19. Memory Per Core

A useful mental model is:

```text
Memory per Core
=
Executor Memory / Executor Cores
```

For example:

```text
8 GB / 4 cores
= 2 GB per core
```

Another configuration:

```text
8 GB / 8 cores
= 1 GB per core
```

The second configuration allows more concurrent tasks but gives each core a smaller share of executor memory.

This is not a strict allocation rule, but it is a useful way to reason about resource pressure.

---

# 20. GC Troubleshooting

Suppose Spark UI shows:

```text
Task Duration = 120 seconds
GC Time       = 90 seconds
```

That is a warning sign.

The executor is spending most of its time doing garbage collection.

Possible causes include:

* Large JVM object creation
* Excessive object overhead
* Too little heap for the workload
* Poor serialization
* Large cached objects

Before simply increasing memory, inspect the workload and object creation patterns.

---

# 21. Practical Memory Troubleshooting Example

Suppose your Spark job fails with:

```text
java.lang.OutOfMemoryError: Java heap space
```

Use this process.

### Step 1 — Find the failing stage

Open:

```text
Spark UI → Stages
```

Identify the failed stage.

---

### Step 2 — Inspect task distribution

Look at:

```text
Task Duration
Input Size
Shuffle Read
Shuffle Write
```

Ask:

```text
Is one task much larger than others?
```

If yes:

```text
Potential data skew
```

---

### Step 3 — Check spill

Look for:

```text
Memory Bytes Spilled
Disk Bytes Spilled
```

Large spill:

```text
Potential memory pressure
```

---

### Step 4 — Check GC

Look at:

```text
GC Time
```

High GC:

```text
Potential JVM memory/object pressure
```

---

### Step 5 — Check the operation

Was the stage doing:

```text
join?
groupBy?
orderBy?
window?
broadcast?
```

These operations can have significant memory requirements.

---

### Step 6 — Check partitioning

Ask:

```text
Are partitions too large?
Are partitions skewed?
Are there too few shuffle partitions?
```

---

### Step 7 — Tune

Only now consider:

```text
executor memory
memory overhead
partition count
executor cores
join strategy
caching strategy
```

---

# 22. Practical Tuning Example

Suppose:

```text
Dataset = 500 GB

Executors = 5
Memory = 4 GB
Cores = 4
```

The application shows:

```text
High Disk Spill
High Task Duration
```

A poor response would be:

```text
Increase memory to 32 GB
```

Instead investigate.

Suppose Spark UI shows:

```text
Large shuffle partitions
```

Then increasing:

```text
spark.sql.shuffle.partitions
```

may reduce per-task data volume.

For example:

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    800
)
```

Then compare:

```text
Before
───────
Large partitions
High spill
Slow tasks

After
─────
Smaller partitions
Lower spill
Faster tasks
```

The exact value should be determined from the workload rather than blindly using `800`.

---

# 23. Practical Tuning Example: PySpark

Suppose:

```text
Executor Memory = 4 GB
Memory Overhead = 512 MB
```

and the application fails with:

```text
Container killed for exceeding memory limits
```

If Spark UI and logs indicate Python/native memory pressure, increasing:

```text
spark.executor.memoryOverhead
```

may be appropriate.

For example:

```bash
--conf spark.executor.memoryOverhead=1024
```

or:

```bash
--conf spark.executor.memoryOverhead=2048
```

The correct value depends on the workload and cluster resources.

The important lesson:

> Do not increase JVM heap when the actual problem is outside the JVM heap.

---

# 24. Practical Tuning Example: Cache

Suppose a DataFrame is used five times:

```python
df.cache()

df.filter(...).count()
df.groupBy(...).count()
df.select(...).show()
df.filter(...).count()
df.groupBy(...).count()
```

Caching may be beneficial because the DataFrame is reused.

But if:

```python
df.cache()

df.count()

# Never use df again
```

then caching provides little benefit.

Better:

```python
df.count()
```

without caching.

---

# 25. A Simple Decision Tree

Use this whenever you see a memory issue:

```text
Memory Problem
      │
      ▼
Driver or Executor?
      │
 ┌────┴────┐
 ▼         ▼
Driver   Executor
 │          │
 │          ├── JVM Heap OOM?
 │          │       │
 │          │       ├── Large partition?
 │          │       ├── Skew?
 │          │       ├── Join?
 │          │       ├── Aggregation?
 │          │       └── Cache?
 │          │
 │          └── Container killed?
 │                  │
 │                  ├── Python memory?
 │                  ├── Native memory?
 │                  └── Memory overhead?
 │
 └── collect()?
     toPandas()?
     Large driver result?
```

---

# 26. What NOT to Do

Avoid blindly applying these fixes:

```text
❌ Increase executor memory every time
❌ Increase driver memory every time
❌ Cache every DataFrame
❌ Broadcast every small-looking table
❌ Increase shuffle partitions indefinitely
❌ Add more executors without checking skew
❌ Use collect() for large results
```

Instead:

```text
✔ Understand the workload
✔ Inspect Spark UI
✔ Check partition distribution
✔ Check shuffle
✔ Check spill
✔ Check GC
✔ Check skew
✔ Tune the actual bottleneck
```

---

# 27. Production-Oriented Memory Checklist

Before deploying a Spark application, ask:

### Data

```text
□ How large is the input?
□ How many partitions are there?
□ Are partitions balanced?
```

### Transformations

```text
□ Are there large joins?
□ Are there aggregations?
□ Are there expensive windows?
□ Are there global sorts?
```

### Caching

```text
□ Is caching actually needed?
□ How much data is being cached?
□ Can unused caches be unpersisted?
```

### Driver

```text
□ Is collect() being used?
□ Is toPandas() being used?
□ Could the result become large?
```

### Executors

```text
□ Is executor memory sufficient?
□ Is memory overhead sufficient?
□ Are executor cores reasonable?
```

### Shuffle

```text
□ Is shuffle volume high?
□ Are partitions too large?
□ Is there data skew?
□ Is AQE enabled?
```

### Monitoring

```text
□ Have Spark UI metrics been checked?
□ Is GC time reasonable?
□ Is disk spill reasonable?
□ Are executors balanced?
```

---

# 28. Golden Rule of Spark Memory Management

The most important rule is:

> **Do not solve a memory problem before understanding why the memory is being consumed.**

For example:

```text
OOM
 │
 ├── Large partition?
 │      → Fix partitioning
 │
 ├── Data skew?
 │      → Fix skew
 │
 ├── Large broadcast?
 │      → Reconsider broadcast
 │
 ├── Too much cache?
 │      → Unpersist / reduce caching
 │
 ├── Driver collect?
 │      → Keep processing distributed
 │
 ├── Python memory?
 │      → Review memory overhead
 │
 └── Genuine heap shortage?
        → Consider more executor memory
```

---

# 29. Final Mental Model

When tuning Spark memory, think across four layers:

```text
                Spark Memory
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
      Driver      Executor     Cluster
        │            │
        │            ├── JVM Heap
        │            │
        │            ├── Execution
        │            │
        │            ├── Storage
        │            │
        │            └── Overhead
        │
        ▼
   Driver Results
```

Then connect memory with:

```text
Memory
  │
  ├── Partitions
  ├── Parallelism
  ├── Shuffle
  ├── Joins
  ├── Aggregations
  ├── Caching
  ├── Data Skew
  └── Garbage Collection
```

This is the practical way to think about Spark memory.

---

# Key Takeaways

1. Start troubleshooting with the **root cause**, not configuration changes.
2. `spark.executor.memory` controls JVM heap memory per executor.
3. `spark.executor.memoryOverhead` handles additional container memory requirements.
4. PySpark applications may need significant memory outside the JVM heap.
5. Large `collect()` and `toPandas()` operations can cause driver OOM.
6. Cache only DataFrames that are actually reused.
7. Use `unpersist()` when cached data is no longer required.
8. Large joins and aggregations can create significant execution-memory pressure.
9. Broadcast joins can reduce shuffle but should only be used when the broadcast side is sufficiently small.
10. Data skew can cause one executor to run out of memory while others remain underutilized.
11. Increasing partition count can reduce per-task memory requirements, but excessive partitions create overhead.
12. `spark.sql.shuffle.partitions` is an important tuning parameter for shuffle-heavy workloads.
13. AQE can dynamically improve query execution based on runtime statistics.
14. High GC time indicates JVM memory/object pressure.
15. Excessive disk spill usually indicates memory pressure or inefficient partitioning.
16. More memory or more executors are **not universal solutions**.
17. Spark UI should be your first source of evidence when troubleshooting performance.
18. Effective memory tuning is a combination of **code optimization + partitioning + execution strategy + resource configuration**.

---
