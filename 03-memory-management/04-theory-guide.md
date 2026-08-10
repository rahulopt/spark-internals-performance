# 04 — Memory Management: Theory Guide

## Overview

Spark memory management is about understanding **where memory is allocated, what consumes it, and what happens when memory becomes insufficient**.

A simple way to think about Spark memory is:

```text
Spark Application
│
├── Driver
│   └── Driver Memory
│
└── Executors
    │
    ├── JVM Heap
    │   │
    │   └── Unified Memory
    │       ├── Execution Memory
    │       └── Storage Memory
    │
    └── Memory Overhead
```

The important thing is not memorizing every configuration.

The important thing is understanding the relationship between:

```text
Memory
+
Partitions
+
Tasks
+
Shuffle
+
Caching
+
Data Distribution
```

---

# 1. Why Does Spark Need Memory?

Spark is a distributed processing engine.

When a task processes data, it needs memory for:

* Reading and processing records
* Sorting
* Joining
* Aggregating
* Shuffling
* Caching
* Maintaining intermediate structures

For example:

```python
df.groupBy("customer_id").sum("revenue")
```

Spark may need memory for intermediate aggregation state.

Similarly:

```python
df.orderBy("revenue")
```

requires resources for sorting.

And:

```python
df.join(customers_df, "customer_id")
```

may require memory for join-related processing.

---

# 2. Driver vs Executor Memory

The first major distinction is:

```text
Driver Memory
        vs
Executor Memory
```

### Driver

The driver coordinates the application.

It manages:

* SparkSession
* Job scheduling
* Query planning
* Application metadata
* Task scheduling

The driver should generally **not** hold the complete dataset.

---

### Executor

Executors perform distributed computation.

They process:

* Input partitions
* Shuffle data
* Joins
* Aggregations
* Sorts
* Cached partitions

Therefore:

> Most data-processing memory consumption happens on executors.

---

# 3. The Executor Memory Model

An executor can be viewed as:

```text
Executor
│
├── JVM Heap
│
└── Memory Overhead
```

The JVM heap contains Spark's primary managed memory.

The memory overhead covers additional container/process memory requirements.

For PySpark:

```text
Executor
│
├── JVM
│
└── Python Worker
```

Therefore, the total memory requirement can be larger than the JVM heap alone.

---

# 4. Unified Memory

Inside Spark's JVM heap, Spark uses a unified memory model.

Conceptually:

```text
Unified Memory
│
├── Execution Memory
│
└── Storage Memory
```

These are not two completely independent fixed pools.

Spark allows them to share memory dynamically.

---

# 5. Execution Memory

Execution memory is used while Spark is **performing computation**.

Typical consumers include:

```text
Execution Memory
│
├── Aggregation
├── Sorting
├── Joins
└── Shuffle-related processing
```

Example:

```python
df.groupBy("region").agg(
    F.sum("revenue")
)
```

Spark needs memory while maintaining intermediate aggregation state.

---

# 6. Storage Memory

Storage memory is primarily associated with persisted data.

For example:

```python
df.cache()
```

or:

```python
df.persist()
```

Conceptually:

```text
DataFrame
    │
    ▼
Storage Memory
    │
    ├── Partition 1
    ├── Partition 2
    └── Partition 3
```

The purpose is to avoid recomputing data that will be reused.

---

# 7. Execution and Storage Are Related

The most important theoretical idea is:

```text
Execution Memory ↔ Storage Memory
```

They operate within the unified memory region.

Suppose cached data is occupying storage memory:

```text
Storage
████████████
```

and a new operation requires more execution memory.

Spark can reclaim eligible storage memory when necessary.

Therefore:

```text
Execution needs memory
        ↓
Storage blocks may be evicted
        ↓
Execution gets more space
```

This dynamic sharing is a fundamental part of Spark's memory model.

---

# 8. Cache Is Not Permanent Memory

A common misunderstanding is:

> "If I call `cache()`, Spark will keep the entire DataFrame permanently in RAM."

This is incorrect.

Caching is a **performance optimization**, not a guarantee that all partitions remain in memory forever.

If cached blocks are evicted:

```text
Cached Partition
      ↓
Evicted
      ↓
Needed again
      ↓
Recomputed from lineage
```

Therefore:

```text
cache()
```

does not break Spark's lineage.

---

# 9. Memory Pressure

Memory pressure occurs when the workload demands more memory than is conveniently available.

Conceptually:

```text
Workload
   │
   ▼
Memory Requirement
   │
   │
   ▼
Available Memory
```

If:

```text
Required Memory > Available Memory
```

Spark may:

```text
Spill
Evict
Garbage Collect
```

or eventually fail if memory requirements cannot be satisfied.

---

# 10. Spill

Spilling means writing intermediate data from memory to disk.

Conceptually:

```text
Computation
     │
     ▼
Memory
     │
     │ insufficient
     ▼
Disk Spill
     │
     ▼
Continue Processing
```

Spilling allows Spark to process data that cannot entirely fit in memory.

However:

```text
Memory
  >
Disk
```

in terms of access speed.

Therefore excessive spilling usually indicates a performance problem.

---

# 11. Out-of-Memory

There are different kinds of memory failures.

### Driver OOM

Usually associated with operations such as:

```python
df.collect()
```

or:

```python
df.toPandas()
```

when the result is too large.

---

### Executor JVM OOM

Possible causes:

* Large partitions
* Large aggregations
* Large joins
* Excessive caching
* Large broadcast data
* Data skew

---

### Container Memory Failure

The total process/container memory can exceed the allocated limit.

This becomes particularly important in PySpark because Python workers consume memory outside the JVM heap.

---

# 12. On-Heap vs Off-Heap

### On-Heap

Memory inside the JVM heap:

```text
JVM
└── Heap
```

It is managed by the JVM and affected by garbage collection.

---

### Off-Heap

Memory outside the JVM heap:

```text
JVM
├── Heap
└── Outside Heap
```

Spark supports off-heap memory for specific memory-management scenarios.

The key distinction is:

```text
On-Heap
    ↓
JVM managed

Off-Heap
    ↓
Outside JVM heap
```

---

# 13. Garbage Collection

Spark executors run inside JVMs.

Therefore, JVM garbage collection can affect Spark performance.

Conceptually:

```text
Objects Created
      ↓
Heap Usage Increases
      ↓
Garbage Collector
      ↓
Unused Objects Removed
```

If garbage collection consumes a large portion of task execution time:

```text
More GC
   ↓
Less useful computation
   ↓
Slower application
```

High GC time is therefore a useful performance signal.

---

# 14. Memory and Partitions

Spark processes data using partitions.

For example:

```text
Dataset
│
├── Partition 1
├── Partition 2
├── Partition 3
└── Partition 4
```

Each partition is processed by tasks.

Therefore:

> Partition size directly influences the amount of data a task may need to process.

If partitions are too large:

```text
Large Partition
      ↓
Large Task
      ↓
High Memory Requirement
```

If partitions are too small:

```text
Tiny Partitions
      ↓
Many Tasks
      ↓
Scheduling Overhead
```

Good partitioning is therefore a balance.

---

# 15. Memory and Parallelism

Suppose an executor has:

```text
4 cores
```

It can potentially execute multiple tasks concurrently.

Conceptually:

```text
Executor
│
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task
```

If every task requires significant memory, simultaneous execution increases memory pressure.

Therefore:

```text
More Concurrent Tasks
        ↓
Higher Potential Memory Demand
```

This is why executor memory and executor cores should not be considered independently.

---

# 16. Memory and Shuffle

Shuffle is one of the most important sources of memory and I/O pressure.

Operations that can trigger shuffle include:

```text
groupBy
join
distinct
orderBy
repartition
```

Conceptually:

```text
Partitioned Data
       │
       ▼
    Shuffle
       │
       ▼
New Partition Layout
```

During shuffle, Spark may need:

* Memory
* Network bandwidth
* Disk
* CPU

Therefore:

> Shuffle optimization is also memory optimization.

---

# 17. Memory and Data Skew

Data skew occurs when data is distributed unevenly.

Example:

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 50 GB
```

The cluster may have enough total memory.

But the task processing Partition 4 may fail.

```text
Executor 1 → Fine
Executor 2 → Fine
Executor 3 → Fine
Executor 4 → Memory Pressure
```

This leads to an important principle:

> Cluster-level memory can be sufficient while task-level memory is insufficient.

---

# 18. Memory and Joins

Different join strategies have different memory implications.

For example:

```python
large_df.join(small_df, "id")
```

may use a broadcast join if the small side is appropriate for broadcasting.

Conceptually:

```text
Small Dataset
     │
     ▼
 Broadcast
     │
 ┌───┼───┐
 ▼   ▼   ▼
E1  E2  E3
```

This can avoid a large shuffle.

But broadcasting a dataset that is too large can create executor memory pressure.

Therefore:

> A faster join strategy is not automatically a safer memory strategy.

---

# 19. Memory and Caching

Caching trades one resource for another:

```text
Without Cache
     ↓
More Recomputation
     ↓
More CPU

With Cache
     ↓
More Storage Memory
     ↓
Less Recomputation
```

Therefore, caching should be used when:

```text
Data is reused
+
Recomputation is expensive
```

Caching data that is used only once usually provides little benefit.

---

# 20. Memory and the Driver

The driver is especially vulnerable when application code moves large datasets out of the distributed system.

Dangerous examples:

```python
df.collect()
```

```python
df.toPandas()
```

The architecture changes from:

```text
Distributed Processing
```

to:

```text
Distributed Processing
        ↓
Driver
        ↓
Single Machine Memory
```

This defeats one of the main advantages of distributed processing.

---

# 21. Memory Configuration

Important Spark memory configurations include:

```text
spark.executor.memory
spark.executor.memoryOverhead
spark.driver.memory
spark.driver.memoryOverhead
spark.memory.offHeap.enabled
spark.memory.offHeap.size
```

For Spark SQL workloads, another important configuration is:

```text
spark.sql.shuffle.partitions
```

These configurations should be tuned based on workload characteristics.

---

# 22. Memory Is Not the Same as Performance

More memory does not automatically mean:

```text
More memory
    ↓
Faster Spark
```

Performance depends on many interacting factors:

```text
Performance
│
├── Memory
├── CPU
├── Partitions
├── Parallelism
├── Shuffle
├── Network
├── Disk
├── Data Skew
├── Join Strategy
└── Serialization
```

A memory problem may actually be caused by poor partitioning or skew.

---

# 23. The Resource Hierarchy

A useful way to reason about Spark is:

```text
Application
     │
     ▼
Executors
     │
     ▼
Executor Cores
     │
     ▼
Tasks
     │
     ▼
Partitions
```

And memory follows the work:

```text
Partition
    ↓
Task
    ↓
Executor
    ↓
Memory
```

Therefore:

> Understanding partitions is essential for understanding Spark memory.

---

# 24. The Three Questions

Whenever you encounter a Spark memory problem, ask three questions.

### Question 1

**Where is the memory problem?**

```text
Driver?
Executor?
Python Worker?
Container?
```

---

### Question 2

**What is consuming the memory?**

```text
Cache?
Join?
Aggregation?
Shuffle?
Large partition?
Broadcast?
Python objects?
```

---

### Question 3

**Why is it consuming so much memory?**

```text
Skew?
Too few partitions?
Large dataset?
Poor join strategy?
Too much caching?
Driver-side collection?
```

Only after answering these questions should you change configuration.

---

# 25. Memory Troubleshooting Philosophy

Bad approach:

```text
OOM
 ↓
Increase memory
 ↓
Problem solved?
```

Better approach:

```text
OOM
 ↓
Identify failure location
 ↓
Inspect Spark UI
 ↓
Inspect task distribution
 ↓
Check shuffle / spill / GC
 ↓
Check skew
 ↓
Check code
 ↓
Tune configuration if necessary
```

This is the difference between **resource guessing** and **performance engineering**.

---

# 26. The Complete Mental Model

Keep this diagram in mind:

```text
                         Spark Application
                                │
                    ┌───────────┴───────────┐
                    │                       │
                  Driver                Executors
                    │                       │
             Driver Memory           Executor Memory
                                            │
                              ┌─────────────┴─────────────┐
                              │                           │
                         JVM Heap                  Memory Overhead
                              │                           │
                       Unified Memory              Python / Native
                              │
                    ┌─────────┴─────────┐
                    │                   │
               Execution             Storage
                    │                   │
              ┌─────┼─────┐       ┌────┴────┐
              │     │     │       │         │
             Join  Sort  Agg    Cache    Persist
```

Now connect it with execution:

```text
Partitions
    ↓
Tasks
    ↓
Executors
    ↓
Memory
    ↓
Execution / Storage
    ↓
Spill / Eviction / GC
    ↓
Performance
```

---

# 27. Core Principles

### Principle 1

**Memory problems are often symptoms, not root causes.**

---

### Principle 2

**Partition size matters more than total cluster memory for individual tasks.**

---

### Principle 3

**Shuffle is both a performance and memory concern.**

---

### Principle 4

**Caching trades storage memory for reduced recomputation.**

---

### Principle 5

**Driver-side collection can destroy the benefits of distributed processing.**

---

### Principle 6

**Data skew can create memory problems even when total cluster memory is sufficient.**

---

### Principle 7

**Executor memory and executor cores must be considered together.**

---

### Principle 8

**More memory is not always the correct optimization.**

---

# 28. Interview Summary

If asked:

### "Explain Spark memory management."

A concise answer:

> Spark memory management primarily involves driver memory and executor memory. On executors, Spark uses a unified memory model where execution memory supports operations such as joins, aggregations, sorting, and shuffle processing, while storage memory is primarily used for cached and persisted data. Execution and storage can dynamically share the unified memory region. Executors also require memory overhead outside the JVM heap, which is particularly important for PySpark because Python workers consume additional memory. When memory becomes insufficient, Spark can evict cached data or spill intermediate data to disk, while severe memory pressure can result in executor or driver failures.

---

# 29. Final Summary

Spark memory management is not simply:

```text
"How much RAM does Spark have?"
```

It is about understanding:

```text
Where?
  ↓
Driver / Executor

What?
  ↓
Execution / Storage / Overhead

Why?
  ↓
Join / Shuffle / Cache / Skew / Partition

What happens when insufficient?
  ↓
Eviction / Spill / GC / Failure

How do we fix it?
  ↓
Code + Partitioning + Execution Strategy + Configuration
```

The most important mindset is:

> **Understand the workload first. Tune the resources second.**

---

