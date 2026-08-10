# 05 — Memory Management: Interview Questions

## Overview

This document contains interview questions covering Spark memory management from **basic fundamentals to advanced troubleshooting**.

The questions are organized by difficulty:

```text
Basic
  ↓
Intermediate
  ↓
Advanced
  ↓
Scenario-Based
  ↓
Troubleshooting
```

---

# Basic Questions

## 1. What is memory management in Spark?

Memory management in Spark is the process of managing memory used by the **driver and executors** for computation, caching, shuffle processing, intermediate data, and other runtime operations.

The main areas are:

```text
Driver Memory
Executor Memory
Execution Memory
Storage Memory
Memory Overhead
```

---

## 2. What is the difference between driver memory and executor memory?

### Driver Memory

Used by the driver for:

* Application coordination
* Query planning
* Job scheduling
* Application metadata
* Receiving results

### Executor Memory

Used by executors for:

* Processing partitions
* Joins
* Aggregations
* Sorting
* Shuffle
* Cached data

Simple distinction:

```text
Driver
→ Coordinates

Executors
→ Compute
```

---

## 3. What does `spark.executor.memory` mean?

It specifies the JVM heap memory available to **each executor**.

Example:

```text
spark.executor.memory = 8g
```

If there are 5 executors:

```text
5 × 8 GB
= 40 GB executor JVM heap
```

It is **per executor**, not total cluster memory.

---

## 4. What is unified memory in Spark?

Spark uses a unified memory model where execution and storage memory share a common memory region.

```text
Unified Memory
│
├── Execution Memory
└── Storage Memory
```

This allows memory to be used more flexibly instead of maintaining completely isolated fixed pools.

---

## 5. What is execution memory?

Execution memory is used during computation.

Examples:

```text
Join
Aggregation
Sort
Shuffle processing
```

For example:

```python
df.groupBy("customer_id").sum("revenue")
```

requires execution memory for intermediate processing.

---

## 6. What is storage memory?

Storage memory is primarily used for persisted data.

For example:

```python
df.cache()
```

or:

```python
df.persist()
```

Cached partitions are stored using Spark's persistence mechanisms.

---

## 7. Can execution memory and storage memory share memory?

Yes.

Spark's unified memory model allows execution and storage to dynamically share the unified memory region.

Conceptually:

```text
Execution Memory ↔ Storage Memory
```

---

## 8. Does `cache()` guarantee that data stays in RAM?

No.

Cached data can be evicted when memory is needed.

If an evicted partition is required again, Spark can recompute it using the DataFrame's lineage.

---

## 9. What happens when Spark does not have enough memory?

Depending on the situation, Spark may:

```text
Evict cached blocks
       ↓
Spill intermediate data to disk
       ↓
Perform garbage collection
       ↓
Eventually fail if memory remains insufficient
```

---

## 10. What is spilling?

Spilling means writing intermediate data from memory to disk when there is insufficient memory.

```text
Memory
   ↓
Insufficient
   ↓
Disk Spill
```

Spilling allows Spark to process workloads that cannot fit entirely in memory.

However, excessive spilling can significantly reduce performance.

---

# Intermediate Questions

## 11. What is memory overhead?

Memory overhead is additional memory allocated outside the executor JVM heap.

Conceptually:

```text
Executor Container
│
├── JVM Heap
└── Memory Overhead
```

It can be used for things such as:

* Python workers
* Native memory
* JVM non-heap requirements
* Other process/container overhead

---

## 12. Why is memory overhead especially important in PySpark?

PySpark uses Python worker processes in addition to the JVM-based Spark executor.

```text
Executor
│
├── JVM
│
└── Python Worker
```

Therefore:

```text
Total Memory
≠
JVM Heap Only
```

Python workers can consume significant memory outside the JVM heap.

---

## 13. What is `spark.executor.memoryOverhead`?

It controls the additional memory allocated for executor container overhead.

Example:

```bash
--conf spark.executor.memoryOverhead=2048
```

This is particularly relevant when the workload uses significant Python, native, or other non-heap memory.

---

## 14. What is on-heap memory?

On-heap memory is memory inside the JVM heap.

```text
JVM
└── Heap
```

It is managed by the JVM garbage collector.

---

## 15. What is off-heap memory?

Off-heap memory is memory allocated outside the JVM heap.

Spark supports explicit off-heap memory configuration:

```properties
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=...
```

Off-heap memory can reduce some JVM heap pressure in appropriate workloads.

---

## 16. What is garbage collection in Spark?

Spark executors run inside JVMs.

When JVM objects become unused, the garbage collector reclaims their memory.

```text
Objects Created
      ↓
Heap Usage
      ↓
Garbage Collection
      ↓
Unused Objects Removed
```

Excessive GC can reduce application performance.

---

## 17. How does garbage collection affect Spark performance?

Suppose:

```text
Task Duration = 100 seconds
GC Time       = 80 seconds
```

The executor is spending most of its time performing garbage collection rather than useful computation.

This can indicate:

* High object creation
* Large object overhead
* Heap pressure
* Inefficient serialization

---

## 18. Why can a Spark application spill data to disk?

When intermediate data cannot fit comfortably in memory, Spark can spill some of it to disk.

This commonly happens during memory-intensive operations such as:

```text
Sort
Aggregation
Shuffle
```

Spilling prevents immediate failure but introduces disk I/O.

---

## 19. Is spilling always bad?

No.

Spilling is a normal mechanism in Spark.

However:

```text
Small spill
→ Usually acceptable

Excessive spill
→ Performance problem
```

Large amounts of disk spill usually indicate memory or partitioning pressure.

---

## 20. What is the relationship between memory and partitions?

Each Spark task processes a partition.

Therefore, partition size influences task-level memory requirements.

```text
Large Partition
      ↓
Large Task
      ↓
Higher Memory Requirement
```

Too few partitions can therefore cause memory pressure.

---

## 21. Can increasing the number of partitions reduce memory pressure?

Yes.

Suppose:

```text
100 GB / 10 partitions
= 10 GB per partition
```

Increasing to:

```text
100 GB / 1000 partitions
= 100 MB average per partition
```

can reduce the amount of data processed by each task.

But too many partitions create scheduling and task overhead.

---

## 22. What is `spark.sql.shuffle.partitions`?

It controls the default number of partitions used for many Spark SQL/DataFrame shuffle operations.

Example:

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    400
)
```

If the value is too low:

```text
Large Shuffle Partitions
      ↓
High Memory Pressure
```

If it is too high:

```text
Many Small Tasks
      ↓
Scheduling Overhead
```

---

## 23. How does data skew affect memory?

Data skew occurs when data is distributed unevenly across partitions.

Example:

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 50 GB
```

The task processing Partition 4 may run out of memory while other executors remain underutilized.

---

## 24. Can a cluster have enough total memory but still experience OOM?

Yes.

For example:

```text
Total Cluster Memory = 100 GB
```

but one partition contains:

```text
50 GB
```

and the executor processing that partition cannot handle it.

Therefore:

> Total cluster memory does not guarantee sufficient memory for every individual task.

---

## 25. Why can `collect()` cause driver OOM?

`collect()` brings the complete result to the driver.

```python
df.collect()
```

Architecture:

```text
Executors
   │
   ├── Results
   ├── Results
   └── Results
        │
        ▼
      Driver
```

If the result is too large:

```text
Driver Memory
     ↓
OOM
```

---

## 26. Why can `toPandas()` cause driver OOM?

`toPandas()` transfers Spark data to the driver and creates a Pandas DataFrame there.

```python
df.toPandas()
```

Therefore, the driver needs enough memory for the resulting Pandas DataFrame.

It should generally only be used for reasonably small datasets.

---

## 27. What is the relationship between caching and memory?

Caching consumes storage resources.

```text
cache()
   ↓
Storage Memory
```

Caching can reduce recomputation but increases memory pressure.

Therefore:

> Cache data when reuse justifies the memory cost.

---

## 28. What does `unpersist()` do?

It removes cached/persisted data that is no longer needed.

Example:

```python
df.unpersist()
```

This helps release storage resources.

---

## 29. How does a broadcast join affect memory?

A broadcast join sends the small side of a join to executors.

```text
Small Dataset
      ↓
Broadcast
      ↓
All Executors
```

This can avoid a large shuffle.

However, broadcasting a dataset that is too large can cause executor memory pressure.

---

## 30. What is the relationship between executor cores and memory?

Executor cores determine how many tasks can potentially execute concurrently.

For example:

```text
Executor
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task
```

More concurrent tasks can increase simultaneous memory requirements.

Therefore, executor cores and executor memory should be considered together.

---

# Advanced Questions

## 31. Why is "just increase executor memory" not always the right solution?

Because the memory problem may actually be caused by:

```text
Data Skew
Large Partition
Excessive Cache
Large Broadcast
Poor Join Strategy
Too Few Shuffle Partitions
Driver Collection
Python Memory
```

Increasing memory may hide the root cause without solving the underlying problem.

---

## 32. How would you troubleshoot an executor OOM?

I would follow these steps:

```text
1. Identify the failed stage
2. Inspect Spark UI
3. Check task distribution
4. Check partition sizes
5. Check shuffle read/write
6. Check memory/disk spill
7. Check GC time
8. Look for data skew
9. Inspect joins/aggregations/caching
10. Review executor memory and overhead
```

Only then would I change resource configuration.

---

## 33. How would you troubleshoot driver OOM?

First I would check whether the application is moving a large dataset to the driver.

Common suspects:

```python
collect()
toPandas()
large driver-side objects
```

Then I would redesign the operation to keep processing distributed whenever possible.

---

## 34. How would you troubleshoot "Container killed for exceeding memory limits"?

I would distinguish container memory from JVM heap memory.

I would check:

```text
Executor JVM Heap
+
Memory Overhead
+
Python Worker
+
Native / Other Memory
```

For PySpark, I would specifically investigate Python worker memory and executor memory overhead.

---

## 35. How would you identify whether data skew is causing OOM?

Look at Spark UI task metrics.

If most tasks finish quickly but one or a few tasks are dramatically larger:

```text
Task 1 → 5 sec
Task 2 → 6 sec
Task 3 → 5 sec
Task 4 → 900 sec
```

that is a strong indication of skew.

I would then inspect:

* Partition sizes
* Key distribution
* Shuffle read
* Shuffle write

---

## 36. How does shuffle create memory pressure?

Shuffle redistributes data between partitions.

```text
Input Partitions
      ↓
Shuffle
      ↓
Network
      ↓
New Partitions
```

During this process Spark may need memory for:

* Sorting
* Aggregation
* Buffering
* Intermediate data

If memory is insufficient, Spark may spill to disk.

---

## 37. What happens if an executor has lots of memory but very few partitions?

The available memory may not be fully utilized because Spark cannot create enough concurrent tasks to use the resources effectively.

For example:

```text
Executor = 16 cores
Partitions = 4
```

Only a small number of tasks can run.

Therefore:

> Memory alone does not determine Spark performance. Parallelism and partitioning matter too.

---

## 38. Can too many executor cores cause memory pressure?

Yes.

Suppose:

```text
Executor Memory = 8 GB
Executor Cores = 8
```

Many tasks may run concurrently.

If each task has significant memory requirements, total concurrent demand can become high.

Reducing executor cores or increasing memory may help depending on the workload.

---

## 39. What is memory pressure from object overhead?

A dataset can consume much more memory as JVM objects than its raw serialized representation suggests.

For example:

```text
Raw Data
   ↓
Serialized Size = 2 GB

In-memory JVM Objects
   ↓
Potentially much larger
```

This can contribute to heap pressure and GC.

Serialization strategy can therefore influence memory efficiency.

---

## 40. Why can Kryo improve memory efficiency?

Kryo can produce more compact serialization than Java serialization for suitable workloads.

Example:

```properties
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

A more compact representation can reduce serialization overhead and potentially improve performance.

However, Kryo is not a universal fix for memory problems.

---

# Scenario-Based Questions

## 41. A Spark job fails with Java heap OOM during `groupBy`. What would you check?

I would check:

```text
1. Number of shuffle partitions
2. Partition sizes
3. Data skew
4. Aggregation cardinality
5. Shuffle spill
6. Executor memory
7. GC time
```

A `groupBy` can generate large intermediate aggregation state, so I would not immediately increase executor memory.

---

## 42. One task takes 30 minutes while all others finish in 1 minute. What could be happening?

The first suspect would be:

```text
Data Skew
```

One partition may contain significantly more data than others.

I would verify this through Spark UI task metrics and partition/shuffle statistics.

---

## 43. All executors show high disk spill. What could be the reason?

Possible causes include:

* Insufficient execution memory
* Very large shuffle
* Too few shuffle partitions
* Large aggregations
* Large sorts

I would inspect the stage and partition distribution before changing memory.

---

## 44. The driver crashes after `df.collect()`. Why?

Because `collect()` transfers the complete result to the driver.

If the result is too large:

```text
Distributed Dataset
        ↓
Driver
        ↓
Driver OOM
```

The solution is usually to avoid collecting the entire dataset.

---

## 45. A PySpark application gets "container killed" but JVM heap looks fine. Why?

The problem may be outside the JVM heap.

Possible causes:

```text
Python Worker
Native Memory
Memory Overhead
Other Process Memory
```

I would inspect and potentially increase:

```text
spark.executor.memoryOverhead
```

after confirming the actual cause.

---

## 46. You increased executor memory but the job is still slow. Why?

The bottleneck may not be memory.

Possible causes:

```text
Data Skew
Shuffle
Network I/O
Too Few Partitions
Too Many Partitions
GC
CPU
Poor Join Strategy
```

Spark performance optimization requires looking at the complete execution profile.

---

## 47. You have 100 GB of data and only 4 partitions. What is the concern?

Average partition size is:

```text
100 GB / 4
= 25 GB
```

Each task may need to process approximately 25 GB of data.

This can create:

```text
Large Tasks
Memory Pressure
Low Parallelism
Long Task Duration
```

Increasing partitions may help, assuming the data is reasonably distributed.

---

## 48. You have 100,000 tiny partitions. Is that good?

Not necessarily.

Too many tiny partitions can cause:

```text
Many Tasks
     ↓
Scheduling Overhead
     ↓
Task Launch Overhead
     ↓
Poor Performance
```

The goal is not maximum partition count.

The goal is appropriate partition sizing and parallelism.

---

# Expert-Level Questions

## 49. Explain the relationship between memory, partitioning, and task execution.

Spark processes data partition by partition.

Each partition is generally processed by a task.

Therefore:

```text
Partition Size
      ↓
Task Data
      ↓
Task Memory Requirement
```

If partitions are too large, tasks may require excessive memory.

If there are too many tiny partitions, scheduling overhead can increase.

Therefore, good partitioning is fundamental to memory efficiency.

---

## 50. Explain the relationship between memory and shuffle.

Shuffle redistributes data between executors.

During shuffle Spark may need memory for:

```text
Sorting
Aggregation
Buffering
Intermediate Data
```

If available memory is insufficient:

```text
Memory Pressure
      ↓
Spill
      ↓
Disk I/O
      ↓
Slower Execution
```

Therefore:

> Reducing unnecessary shuffle can improve both memory usage and performance.

---

## 51. Why is data skew a memory problem as well as a performance problem?

Skew creates uneven partition sizes.

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 50 GB
```

The executor handling Partition 4 receives a much larger workload.

This can result in:

```text
High Memory Usage
Long Task Duration
Spill
Executor OOM
```

Therefore, skew affects both performance and memory.

---

## 52. Explain why caching can sometimes make an application slower.

Caching consumes storage memory.

If too much data is cached:

```text
Storage Memory
      ↓
Memory Pressure
      ↓
Evictions
      ↓
Recomputation
```

At the same time, execution may need memory for joins or aggregations.

Therefore, unnecessary caching can increase memory pressure and cause recomputation.

---

## 53. Why is driver memory not a substitute for executor memory?

The driver and executors have different responsibilities.

Increasing driver memory does not solve an executor-side problem.

For example:

```text
Executor OOM
```

cannot normally be fixed simply by increasing:

```text
spark.driver.memory
```

Similarly:

```text
Driver OOM
```

is not automatically solved by increasing executor memory.

---

## 54. How would you approach Spark memory tuning in production?

I would follow an evidence-driven process:

```text
1. Identify the failing or slow stage
2. Inspect Spark UI
3. Check executor metrics
4. Check GC
5. Check spill
6. Check shuffle
7. Check partition distribution
8. Identify skew
9. Review joins and caching
10. Review driver-side operations
11. Tune partitions
12. Tune executor resources
13. Re-run and compare metrics
```

The goal is not simply to allocate more memory.

The goal is to remove the actual bottleneck.

---

# Rapid-Fire Revision

## Q55. Is executor memory per executor?

**Yes.**

---

## Q56. Does `cache()` guarantee RAM residency?

**No.**

---

## Q57. Can Spark spill data to disk?

**Yes.**

---

## Q58. Is spilling an error?

**No. Excessive spilling is a performance concern.**

---

## Q59. Can one executor OOM while others are fine?

**Yes, especially because of skew or large partitions.**

---

## Q60. Can `collect()` cause driver OOM?

**Yes.**

---

## Q61. Can `toPandas()` cause driver OOM?

**Yes.**

---

## Q62. Does PySpark use memory outside JVM heap?

**Yes. Python workers can consume additional memory.**

---

## Q63. Does more memory always improve performance?

**No.**

---

## Q64. Does more executors always solve OOM?

**No.**

---

## Q65. Can too many partitions hurt performance?

**Yes.**

---

## Q66. Can too few partitions cause memory pressure?

**Yes.**

---

## Q67. Can data skew cause OOM?

**Yes.**

---

## Q68. Is shuffle related to memory usage?

**Yes.**

---

## Q69. Does broadcast eliminate all memory concerns?

**No. The broadcast dataset must still fit appropriately on executors.**

---

## Q70. What is the most important Spark memory troubleshooting tool?

**Spark UI, combined with application logs and execution metrics.**

---

# Final Interview Cheat Sheet

```text
                    SPARK MEMORY
                         │
          ┌──────────────┴──────────────┐
          │                             │
       DRIVER                       EXECUTOR
          │                             │
   Driver Memory              ┌─────────┴─────────┐
                              │                   │
                          JVM Heap           Overhead
                              │                   │
                       Unified Memory       Python/Native
                              │
                    ┌─────────┴─────────┐
                    │                   │
                Execution            Storage
                    │                   │
             Join / Sort / Agg       Cache
             Shuffle Processing       Persist
```

### When memory is insufficient:

```text
Memory Pressure
      │
      ├── Storage Eviction
      │
      ├── Disk Spill
      │
      ├── GC
      │
      └── OOM / Container Failure
```

### Root causes to investigate:

```text
Large Partitions
Data Skew
Large Shuffle
Large Broadcast
Too Much Cache
Too Few Partitions
Large Driver Result
Python Memory
Insufficient Heap
Insufficient Overhead
```

### Golden Interview Answer

> **"I would not immediately increase memory. First, I would identify whether the issue is on the driver or executor, inspect the Spark UI, check task and partition distribution, shuffle, spill, GC time, and data skew. Then I would determine whether the root cause is related to partitioning, joins, aggregation, caching, Python memory, or actual resource shortage. Only after identifying the bottleneck would I tune executor memory, memory overhead, cores, or partition configuration."**

---
