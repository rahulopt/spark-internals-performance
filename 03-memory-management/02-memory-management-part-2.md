# 02 — Memory Management in Apache Spark (Part 2)

## Overview

In Part 1, we understood the basic Spark memory model:

```text
Spark Application
│
├── Driver
│   └── Driver Memory
│
└── Executors
    ├── JVM Heap
    │   └── Unified Memory
    │       ├── Execution Memory
    │       └── Storage Memory
    │
    └── Memory Overhead
```

In this part, we will move from **concepts to practical memory management**.

We will understand:

* Executor memory configuration
* Memory overhead
* On-heap vs off-heap memory
* PySpark memory
* Memory-related failures
* Spark UI memory diagnosis
* Practical memory tuning

---

# 1. Executor Memory Configuration

The most common Spark configuration is:

```bash
--executor-memory 4g
```

or:

```properties
spark.executor.memory=4g
```

This defines the JVM heap size for each executor.

For example:

```text
4 Executors
×
4 GB executor memory

= 16 GB executor JVM heap
```

Remember:

> `spark.executor.memory` is per executor, not for the entire cluster.

---

# 2. Executor Memory Is Per Executor

Suppose we configure:

```text
spark.executor.instances = 5
spark.executor.memory    = 8g
```

Then:

```text
Executor 1 → 8 GB
Executor 2 → 8 GB
Executor 3 → 8 GB
Executor 4 → 8 GB
Executor 5 → 8 GB
```

Total executor heap:

```text
5 × 8 GB = 40 GB
```

This does **not** mean one executor has 40 GB.

Each executor has its own memory.

---

# 3. Memory Overhead

Executor memory is not the entire memory consumed by an executor container.

Spark also needs memory outside the JVM heap.

This is called:

```text
Memory Overhead
```

Conceptually:

```text
Executor Container Memory
│
├── JVM Heap
│   └── spark.executor.memory
│
└── Memory Overhead
    ├── Python processes
    ├── Native memory
    ├── JVM native structures
    └── Other non-heap usage
```

Therefore:

```text
Total Container Memory
=
Executor Heap
+
Memory Overhead
```

---

# 4. `spark.executor.memoryOverhead`

The configuration is:

```properties
spark.executor.memoryOverhead
```

For example:

```bash
--conf spark.executor.memoryOverhead=1024
```

This specifies additional memory for the executor container.

The exact default calculation depends on Spark version and deployment environment, so it is usually better to let Spark calculate it unless your workload requires tuning.

---

# 5. Why Memory Overhead Matters in PySpark

PySpark uses two major processes:

```text
Executor
│
├── JVM
│   └── Spark Executor
│
└── Python Worker
    └── Python code
```

The Python worker does not simply become part of the JVM heap.

Therefore, a PySpark application may consume significant memory outside:

```text
spark.executor.memory
```

For example:

```text
JVM Heap       → 4 GB
Python Worker  → 1 GB
Other overhead → 0.5 GB
```

The executor container may require approximately:

```text
5.5 GB
```

even though:

```text
spark.executor.memory = 4g
```

---

# 6. A Common PySpark Memory Problem

Suppose:

```text
spark.executor.memory = 4g
```

and the application uses Python-heavy operations.

The executor might consume:

```text
JVM          → 3.5 GB
Python       → 1.2 GB
Other        → 0.5 GB
----------------------
Total        → 5.2 GB
```

If the container limit is lower than the actual usage, the cluster manager may kill the container.

You may see an error similar to:

```text
Container killed for exceeding memory limits
```

This is different from a normal Java heap OOM.

---

# 7. Java Heap OutOfMemoryError vs Container Memory Error

These two problems are often confused.

### Java Heap OOM

Example:

```text
java.lang.OutOfMemoryError: Java heap space
```

This generally indicates that the JVM heap is insufficient.

Possible causes:

* Large objects
* Large aggregation
* Huge broadcast
* Inefficient code
* Too much cached data

---

### Container Memory Limit

Example:

```text
Container killed for exceeding memory limits
```

This can happen when total container memory exceeds the configured limit.

For PySpark, Python worker memory can contribute significantly.

Mental model:

```text
Java Heap OOM
      ↓
JVM heap problem

Container Memory Error
      ↓
Total container memory problem
```

---

# 8. On-Heap Memory

On-heap memory is memory managed by the JVM heap.

Spark's normal JVM-based execution primarily uses heap memory.

Conceptually:

```text
JVM
│
└── Heap
    ├── Spark objects
    ├── Execution data
    └── Storage data
```

This memory is managed by the JVM garbage collector.

---

# 9. Off-Heap Memory

Spark can also use memory outside the JVM heap.

This is called:

```text
Off-Heap Memory
```

It can be enabled/configured using:

```properties
spark.memory.offHeap.enabled=true
```

and:

```properties
spark.memory.offHeap.size=...
```

Conceptually:

```text
Executor
│
├── On-Heap Memory
│
└── Off-Heap Memory
```

Off-heap memory can reduce some JVM heap pressure, but it does **not** mean memory management problems disappear.

---

# 10. Why Use Off-Heap Memory?

One motivation is to reduce pressure on the JVM garbage collector.

Large amounts of JVM heap data can increase:

```text
Garbage Collection
        ↓
GC overhead
        ↓
Longer task execution
```

Off-heap memory can be useful in specific workloads, but it should not be enabled simply because "more memory is better."

It should be used when there is a clear performance or memory-management reason.

---

# 11. Garbage Collection

Because Spark executors run inside JVMs, garbage collection can affect performance.

Consider:

```text
Executor
   │
   ├── Creates many JVM objects
   │
   ▼
Heap becomes full
   │
   ▼
Garbage Collector
   │
   ▼
Cleanup unused objects
```

If the application constantly creates and destroys large numbers of objects:

```text
More Objects
    ↓
More GC
    ↓
More CPU spent on GC
    ↓
Less CPU available for Spark processing
```

This is called **GC pressure**.

---

# 12. Serialization and Memory

Spark also needs to represent data in memory.

Poorly serialized data can consume more memory.

For example:

```text
Python / JVM Objects
       ↓
Large memory footprint
```

Serialization can make data more compact and reduce object overhead.

Spark supports serializers such as:

```text
JavaSerializer
KryoSerializer
```

Kryo is often more compact and faster than Java serialization for suitable workloads.

Example:

```properties
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

However, changing serializers is a tuning decision, not a universal requirement.

---

# 13. Memory During Shuffle

Shuffle operations are major consumers of execution resources.

Examples:

```python
df.groupBy("customer_id").count()
```

```python
df.join(other_df, "customer_id")
```

```python
df.orderBy("revenue")
```

Conceptually:

```text
Input Partitions
      │
      ▼
Shuffle
      │
      ├── Memory
      │
      └── Disk Spill
             │
             ▼
        Shuffle Files
```

When shuffle data becomes too large for memory, Spark can spill intermediate data to disk.

---

# 14. Spill Is Not Always an Error

This is important.

Seeing spill in Spark does not automatically mean the application is broken.

For example:

```text
Memory
  ↓
Insufficient
  ↓
Spill to Disk
  ↓
Continue
```

Spark can continue processing.

However:

```text
Too Much Spill
      ↓
Large Disk I/O
      ↓
Slow Tasks
```

Therefore, spill is often a **performance signal**, not necessarily a failure.

---

# 15. Cache and Memory Pressure

Suppose we execute:

```python
df.cache()
```

and then:

```python
df.count()
```

Spark materializes the cache.

Now imagine:

```text
Executor Storage Memory
│
├── Partition 1
├── Partition 2
├── Partition 3
└── Partition 4
```

A large shuffle operation starts.

Execution memory requires more space.

Spark may need to evict cached blocks.

```text
Storage
   │
   │ eviction
   ▼
Free Memory
   │
   ▼
Execution
```

If an evicted partition is required later, Spark may recompute it.

---

# 16. Why Caching Too Much Is Dangerous

Consider:

```python
df1.cache()
df2.cache()
df3.cache()
df4.cache()
df5.cache()
```

If these DataFrames are large:

```text
Storage Memory
      ↓
Fills up
      ↓
Evictions
      ↓
Recomputation
      ↓
More CPU
```

Caching everything can therefore make an application **slower**, not faster.

Good practice:

> Cache only data that is reused and expensive to recompute.

---

# 17. Broadcast Variables and Memory

Broadcast joins can also affect memory.

Example:

```python
large_df.join(
    F.broadcast(small_df),
    "product_id"
)
```

Spark broadcasts the small dataset to executors.

Conceptually:

```text
Driver
  │
  │ Broadcast
  ▼
Executor 1 → Small Dataset
Executor 2 → Small Dataset
Executor 3 → Small Dataset
Executor 4 → Small Dataset
```

The broadcast data needs to fit reasonably within executor resources.

Broadcasting a dataset that is too large can create memory pressure.

---

# 18. Large `collect()` Is Dangerous

One of the most common mistakes is:

```python
df.collect()
```

Suppose:

```text
Data = 100 GB
```

and you execute:

```python
df.collect()
```

Spark attempts to bring the result to the driver.

Conceptually:

```text
Executor 1 ─┐
Executor 2 ─┤
Executor 3 ─┼──→ Driver
Executor 4 ─┤
Executor 5 ─┘
```

This can overwhelm driver memory.

Instead, prefer operations that keep computation distributed.

For example:

```python
df.groupBy("category").count().show()
```

rather than collecting the entire dataset.

---

# 19. `show()` vs `collect()`

This is an important practical distinction.

```python
df.show(10)
```

displays a limited number of rows.

Whereas:

```python
df.collect()
```

returns the entire result to the driver.

For large datasets:

```text
show()
   ↓
Small result to driver

collect()
   ↓
Potentially huge result to driver
```

---

# 20. Spark UI for Memory Diagnosis

Spark UI is one of the best tools for understanding memory and performance.

Important tabs include:

```text
Jobs
Stages
Storage
Executors
SQL / SQL Details
Environment
```

For memory problems, pay particular attention to:

```text
Executors
Storage
Stages
SQL
```

---

# 21. Executors Tab

The Executors tab can help you inspect executor-level information.

You may see metrics related to:

* Memory used
* Disk used
* Task activity
* GC time
* Shuffle read
* Shuffle write
* Executor status

For example:

```text
Executor
│
├── Tasks
├── Memory
├── GC Time
├── Shuffle Read
└── Shuffle Write
```

If one executor behaves very differently from the others, that can indicate data skew.

---

# 22. Storage Tab

The Storage tab is especially useful when using:

```python
cache()
```

or:

```python
persist()
```

You can inspect cached datasets and their storage levels.

Conceptually:

```text
Storage
│
├── DataFrame A
│   ├── Cached Partitions
│   └── Memory Used
│
└── DataFrame B
    ├── Cached Partitions
    └── Memory Used
```

This helps answer:

> "What is consuming my storage memory?"

---

# 23. Stage-Level Spill Metrics

In the Spark UI, stage metrics can reveal spill.

You may see metrics such as:

```text
Memory Bytes Spilled
Disk Bytes Spilled
```

Suppose:

```text
Memory Spill = 500 MB
Disk Spill   = 20 GB
```

This is a strong indication that the operation is under significant memory pressure.

The next step is not automatically "increase memory."

First investigate:

* Partition count
* Data skew
* Join strategy
* Aggregation size
* Shuffle volume

---

# 24. Data Skew and Memory

Memory problems can sometimes be caused by **data skew**, not insufficient total memory.

Suppose:

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 80 GB
```

Total data might technically fit across the cluster.

But Executor 4 processing Partition 4 may run out of memory.

```text
Executor 1 → Fine
Executor 2 → Fine
Executor 3 → Fine
Executor 4 → OOM
```

This is a skew problem.

---

# 25. More Executors May Not Fix Skew

Suppose you increase:

```text
10 executors
```

to:

```text
30 executors
```

but one key still owns most of the data.

You might still get:

```text
One Huge Partition
        ↓
One Task
        ↓
One Executor
        ↓
Memory Pressure
```

Therefore:

> Adding cluster resources does not automatically solve data skew.

You may need to redesign the partitioning or join strategy.

---

# 26. Practical Memory Tuning Checklist

When Spark has memory problems, follow this order.

### Step 1 — Identify where the problem occurs

Is it:

```text
Driver?
Executor?
Python Worker?
Container?
```

---

### Step 2 — Check Spark UI

Look for:

```text
GC Time
Spill
Shuffle
Task duration
Executor imbalance
Cached data
```

---

### Step 3 — Check Data Distribution

Ask:

```text
Are partitions balanced?
Is there a skewed key?
```

---

### Step 4 — Check the Operation

Identify whether the problem is caused by:

```text
Join
GroupBy
Sort
Window
Broadcast
Cache
Collect
```

---

### Step 5 — Tune the Configuration

Only after understanding the problem, consider:

```text
executor memory
memory overhead
executor cores
number of executors
partition count
```

---

# 27. Common Mistakes

## Mistake 1 — Increasing Memory Blindly

Bad approach:

```text
OOM
 ↓
Increase executor memory
 ↓
OOM again
 ↓
Increase again
```

Instead, identify why memory is being consumed.

---

## Mistake 2 — Caching Everything

```python
df1.cache()
df2.cache()
df3.cache()
```

Caching should be intentional.

---

## Mistake 3 — Using `collect()` on Large Data

```python
df.collect()
```

can move a huge amount of data to the driver.

---

## Mistake 4 — Ignoring Memory Overhead

Especially in PySpark:

```text
JVM Heap ≠ Total Process Memory
```

---

## Mistake 5 — Ignoring Data Skew

A single huge partition can cause an executor OOM even when the cluster has plenty of total memory.

---

# 28. A Practical Example

Suppose we run:

```python
result = (
    orders_df
    .join(customers_df, "customer_id")
    .groupBy("region")
    .agg(F.sum("revenue"))
)
```

Possible memory requirements:

```text
Join
 ↓
Shuffle
 ↓
Aggregation
 ↓
Shuffle / Sort
```

If memory is insufficient:

```text
Execution Memory
      ↓
Pressure
      ↓
Spill
      ↓
Disk I/O
      ↓
Slow Stage
```

If a customer key is heavily skewed:

```text
One partition becomes huge
      ↓
One task processes huge data
      ↓
Executor memory pressure
```

Therefore, the solution could be:

```text
Not necessarily more memory
```

It could instead be:

```text
Better partitioning
+
Skew handling
+
Better join strategy
+
Appropriate partition count
```

---

# 29. Memory Troubleshooting Flow

Use this mental flow whenever you encounter a Spark memory issue:

```text
             Spark Memory Problem
                      │
                      ▼
             Where did it fail?
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
       Driver                  Executor
          │                       │
          ▼                       ▼
     collect()?            JVM Heap / Overhead?
                                  │
                                  ▼
                            Check Spark UI
                                  │
                    ┌─────────────┼─────────────┐
                    ▼             ▼             ▼
                  Spill         GC Time        Skew
                    │             │             │
                    ▼             ▼             ▼
                Partitions     Objects       Partitioning
                / Shuffle      / Memory      / Join
```

---

# 30. Interview Questions

### Q1. What does `spark.executor.memory` represent?

It represents the JVM heap memory allocated to each Spark executor.

---

### Q2. Is executor memory the total memory consumed by an executor?

No.

The executor may also require memory overhead and other non-heap resources.

---

### Q3. Why is memory overhead important in PySpark?

Because Python worker processes can consume memory outside the JVM heap.

---

### Q4. What is the difference between heap and off-heap memory?

Heap memory is managed within the JVM heap and is subject to JVM garbage collection.

Off-heap memory exists outside the JVM heap and can be used for specific Spark memory-management scenarios.

---

### Q5. What is spilling?

When an operation cannot keep all intermediate data in memory, Spark can write some intermediate data to disk and continue processing.

---

### Q6. Is spilling always bad?

No.

Spilling is a normal mechanism that allows Spark to process datasets larger than available memory.

However, excessive spilling can significantly reduce performance.

---

### Q7. Can adding more executors solve every memory problem?

No.

Problems such as:

* Data skew
* Driver OOM
* Large broadcast
* Excessive caching
* Bad partitioning

may require a different solution.

---

### Q8. Why can one executor run out of memory while others are fine?

Usually because one or more partitions are significantly larger than the others.

This can happen due to data skew.

---

### Q9. Why is `collect()` dangerous?

Because it moves the complete result to the driver and can exceed driver memory.

---

### Q10. How would you troubleshoot a Spark OOM?

I would:

1. Identify whether the failure is on the driver or executor.
2. Inspect the Spark UI.
3. Check GC and spill metrics.
4. Check partition sizes and data skew.
5. Inspect joins, aggregations, caching, and broadcast operations.
6. Review executor memory and memory overhead.
7. Tune configuration only after identifying the actual bottleneck.

---

# Key Takeaways

1. `spark.executor.memory` is **per executor**.
2. Executor JVM heap is different from total container memory.
3. `spark.executor.memoryOverhead` provides additional non-heap/container memory.
4. PySpark can consume significant memory through Python workers.
5. On-heap memory is managed by the JVM.
6. Off-heap memory exists outside the JVM heap.
7. Excessive JVM object creation can cause GC pressure.
8. Shuffle operations can consume significant execution memory.
9. Spark can spill intermediate data to disk when memory is insufficient.
10. Excessive spilling usually means performance degradation.
11. Caching consumes storage memory and should be used selectively.
12. `collect()` can cause driver memory problems.
13. Data skew can cause one executor to run out of memory while others remain underutilized.
14. Increasing memory is not always the correct solution.
15. Spark UI is essential for diagnosing memory and performance problems.
16. Always identify the **root cause** before changing Spark memory configurations.

---


