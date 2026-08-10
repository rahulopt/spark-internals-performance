01 — Memory Management in Apache Spark (Part 1)
Overview

Memory management is one of the most important concepts for understanding Spark performance.

When a Spark application runs, executors need memory to:

Store input and intermediate data
Execute transformations and aggregations
Perform joins and shuffles
Cache/persist DataFrames
Sort and aggregate records
Maintain Spark's internal structures

A basic understanding of Spark memory helps explain errors such as:

java.lang.OutOfMemoryError: Java heap space

and

ExecutorLostFailure
1. Where Does Spark Memory Exist?

A Spark application primarily has two types of processes:

Spark Application
│
├── Driver
│
└── Executors
    ├── Executor 1
    ├── Executor 2
    └── Executor 3

Both the Driver and Executors use memory, but their responsibilities are different.

Driver Memory

The driver is responsible for:

Creating the SparkSession
Building the logical/physical plan
Scheduling jobs
Maintaining application metadata
Receiving results from executors

For example:

result = df.collect()

collect() brings all records back to the driver.

If the result is very large:

Executors
   │
   │  All records
   ▼
Driver Memory
   │
   X
OutOfMemoryError

Therefore, collecting a large DataFrame can cause driver memory problems.

2. Executor Memory

Executors perform the actual data processing.

Each executor has its own JVM process and memory.

For example:

Executor
│
├── JVM Heap
│   ├── Execution Memory
│   └── Storage Memory
│
└── Off-Heap Memory

Executor memory is particularly important because most Spark computation happens there.

3. Executor Memory Configuration

A common Spark configuration is:

--executor-memory 4g

This means each executor gets approximately:

4 GB JVM heap

For example:

Executor 1 → 4 GB
Executor 2 → 4 GB
Executor 3 → 4 GB

Total executor heap memory:

3 × 4 GB = 12 GB

However, this is not the complete memory available to an executor.

Spark also needs additional memory outside the JVM heap.

4. Spark Executor Memory Model

A simplified view is:

Executor Container
│
├── JVM Heap
│   │
│   ├── Reserved Memory
│   │
│   └── Unified Memory
│       │
│       ├── Execution Memory
│       │
│       └── Storage Memory
│
└── Memory Overhead

This distinction is important when running Spark on cluster managers such as:

YARN
Kubernetes
Amazon EMR
5. Reserved Memory

Spark internally keeps a small amount of memory reserved for system-level operations.

Historically, Spark had a fixed reserved-memory concept that protected the JVM from using the entire heap.

The important idea is:

Executor Heap
│
├── Reserved
│
└── Usable Spark Memory

You generally don't tune reserved memory during normal Spark application development.

The more important area is Unified Memory.

6. Unified Memory

Modern Spark uses a unified memory model.

Unified memory is divided into two major regions:

Unified Memory
│
├── Execution Memory
│
└── Storage Memory

The key point is that these two areas can dynamically borrow memory from each other.

This is different from thinking of execution and storage as two completely isolated blocks.

7. Execution Memory

Execution memory is used during computation.

Examples include:

Sort
df.orderBy("unit_price")

Spark needs memory while sorting records.

Join
orders.join(customers, "customer_id")

Depending on the physical join strategy, Spark may need memory for structures such as hash tables.

Aggregation
df.groupBy("customer_id").sum("revenue")

Spark needs memory to maintain intermediate aggregation state.

Shuffle

Operations such as:

groupBy()
join()
distinct()
orderBy()

can create shuffle data.

Execution memory can therefore be heavily used during these operations.

8. Storage Memory

Storage memory is primarily used for cached or persisted data.

For example:

df.cache()

or:

df.persist()

When Spark caches a DataFrame:

DataFrame
   │
   ▼
Storage Memory

Spark stores cached partitions so that future operations can reuse them instead of recomputing them.

9. Execution vs Storage

Consider:

Executor Memory
│
└── Unified Memory
    │
    ├── Execution
    │   ├── Sort
    │   ├── Join
    │   ├── Aggregation
    │   └── Shuffle processing
    │
    └── Storage
        ├── cache()
        ├── persist()
        └── Broadcast blocks

A useful mental model is:

Execution memory helps Spark compute. Storage memory helps Spark remember.

10. The Important Part: Memory Can Be Borrowed

Suppose unified memory has:

10 GB

Conceptually:

Unified Memory
│
├── Execution
└── Storage

If execution currently needs more memory while storage is using less, execution can make use of available storage memory.

Similarly, storage can use memory that execution is not currently using.

Therefore:

Execution Memory ↔ Storage Memory

They are not permanently isolated pools.

11. What Happens When Memory Becomes Tight?

Spark tries to manage memory dynamically.

For example:

Storage Memory
     │
     │ cached data
     ▼
[Partition][Partition][Partition]

Execution suddenly needs more memory
     │
     ▼
Spark attempts to reclaim/evict storage data

If cached data can be evicted, Spark may remove it from memory.

Later, if the DataFrame is needed again, Spark may recompute the missing partition.

This is one reason caching is not equivalent to permanently storing data in memory.

12. Cache Does Not Mean "Everything Must Stay in RAM"

Consider:

df.cache()

This tells Spark:

Persist this DataFrame using the default persistence strategy.

It does not guarantee that every partition will always remain in RAM.

If memory is insufficient, Spark may evict cached blocks.

For example:

Cached DataFrame

Partition 1 → Memory
Partition 2 → Memory
Partition 3 → Memory
Partition 4 → Evicted

If Partition 4 is required again, Spark can recompute it from the lineage.

13. Memory Fraction

Spark historically exposed configuration related to the fraction of JVM heap used for execution and storage.

A commonly discussed configuration is:

spark.memory.fraction

It controls the fraction of the JVM heap available for Spark's unified memory region.

For example, conceptually:

Executor Heap
│
├── Reserved / Other
│
└── Unified Memory

The exact amount available to unified memory depends on Spark's memory configuration and version.

The default value should generally be left unchanged unless there is a specific tuning reason.

14. Storage Fraction

Another historical configuration is:

spark.memory.storageFraction

This defines the portion of unified memory that storage is initially allowed to use.

The important concept is:

It is not a hard permanent boundary between execution and storage.

Execution and storage can share the unified memory region.

15. Example

Suppose an executor has:

8 GB heap

Conceptually:

Executor Heap
│
├── Other / Reserved
│
└── Unified Memory
     │
     ├── Execution
     └── Storage

Now imagine:

Storage → 3 GB
Execution → 2 GB

Then an operation such as a large aggregation suddenly requires more execution memory.

Spark may need to reclaim storage memory.

Therefore:

Before:

Execution → 2 GB
Storage   → 3 GB


After:

Execution → 4 GB
Storage   → 1 GB

The exact behavior depends on the current memory state and Spark's memory-management rules, but the important idea is dynamic sharing.

16. Memory Overhead

One of the most important concepts is that:

executor-memory

is not the same thing as the total memory required by the executor container.

For example:

--executor-memory 4g

does not necessarily mean the executor requires only 4 GB from the cluster.

There is also memory overhead.

Conceptually:

Container Memory
│
├── Executor JVM Heap
│
└── Memory Overhead

Memory overhead can account for things outside the JVM heap, including:

JVM native memory
Python processes
Native libraries
Off-heap allocations
Other non-heap overhead

This becomes especially important with PySpark.

17. PySpark Memory

PySpark adds another layer because Python code runs outside the JVM.

Simplified architecture:

Executor
│
├── JVM
│   └── Spark Executor
│
└── Python Worker
    └── Python code

Therefore, a PySpark executor may need memory for both:

JVM
+
Python Worker

This is one reason memory overhead configuration matters in PySpark applications.

18. Common Memory Problem

Suppose:

--executor-memory 4g

but the Python workload requires significant additional memory.

The executor container may exceed its allowed memory.

This can lead to failures such as:

Container killed for exceeding memory limits

or executor/container failures.

Increasing only:

--executor-memory

is not always the correct solution.

Sometimes the problem is related to:

memoryOverhead
19. Driver Memory vs Executor Memory

A common interview question is:

What is the difference?
Memory	Used By	Typical Problem
Driver Memory	Driver process	Large collect(), huge metadata
Executor Memory	Executors	Large joins, aggregations, caching
Memory Overhead	Container/non-heap/Python	Container killed due to memory limit

Example:

df.collect()

Large result:

Executor
   │
   ▼
Driver
   X
Driver OOM

While:

df.groupBy("customer_id").count()

may cause executor-side memory pressure.

20. Why Memory Management Matters for Performance

Suppose your application performs:

df.groupBy("customer_id").agg(
    F.sum("revenue")
)

If enough memory is available:

Input
  ↓
Shuffle
  ↓
Aggregation
  ↓
Result

If memory becomes insufficient:

Memory Pressure
      ↓
Spilling
      ↓
Disk I/O
      ↓
Slower Task

Therefore, memory pressure can directly affect Spark performance.

21. Memory and Disk Spilling

Spark does not necessarily fail immediately when an operation cannot fit completely in memory.

Some operations can spill intermediate data to disk.

Conceptually:

Execution
   │
   ▼
Memory
   │
   │ insufficient
   ▼
Spill to Disk
   │
   ▼
Continue Processing

Disk is much slower than memory.

Therefore:

Enough Memory
      ↓
Less spilling
      ↓
Better performance

But:

Insufficient Memory
      ↓
More spilling
      ↓
More disk I/O
      ↓
Slower job
22. Does More Memory Always Mean Better Performance?

No.

Simply increasing executor memory does not automatically make Spark faster.

For example:

4 executors × 4 GB

may sometimes perform better than:

1 executor × 16 GB

because Spark performance also depends on:

Number of executors
Number of cores
Number of partitions
Data distribution
Shuffle volume
Join strategy
Serialization
Garbage collection
Network I/O
Disk I/O

Memory is only one part of Spark performance.

23. Practical Example

Suppose we process:

1 TB dataset

with:

10 executors
4 cores/executor
8 GB executor memory

A simplified architecture:

                    Driver
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
   Executor 1    Executor 2    Executor 3
      ...            ...           ...
        │
        ▼
   Executor 10

Each executor has its own memory.

The total cluster memory is approximately:

10 × 8 GB = 80 GB

plus memory overhead and other cluster resources.

The data is distributed across partitions, so no single executor necessarily needs to hold the entire 1 TB dataset.

24. Key Mental Model

Keep this model in mind:

                 Spark Application
                        │
            ┌───────────┴───────────┐
            │                       │
          Driver                 Executors
            │                       │
      Driver Memory          Executor Memory
                                    │
                       ┌────────────┴────────────┐
                       │                         │
                 JVM Heap                  Memory Overhead
                       │
                Unified Memory
                       │
             ┌─────────┴─────────┐
             │                   │
        Execution             Storage
             │                   │
       ┌─────┼─────┐       ┌─────┼─────┐
       │     │     │       │     │     │
     Join  Sort  Agg     Cache Persist Broadcast
25. Interview Questions
Q1. What is executor memory?

Memory allocated to each Spark executor JVM for processing and storing data.

Q2. What is the difference between execution and storage memory?

Execution memory is used for computation such as joins, sorting, and aggregations.

Storage memory is used primarily for cached and persisted data.

Q3. Can execution and storage memory share memory?

Yes. Spark's unified memory model allows execution and storage to dynamically use available memory rather than treating them as completely independent fixed pools.

Q4. Does cache() guarantee that data remains in memory?

No.

Cached data can be evicted when memory is required. Spark can recompute evicted partitions from lineage.

Q5. Why can a PySpark application require memory overhead?

Because PySpark uses Python worker processes in addition to the JVM-based Spark executor.

Q6. What is the difference between driver memory and executor memory?

Driver memory is used by the driver process for application coordination and metadata.

Executor memory is used by executors to perform distributed data processing.

Q7. What happens when an operation does not fit in memory?

Depending on the operation, Spark may spill intermediate data to disk instead of keeping everything in memory.

This can significantly reduce performance because disk I/O is slower than memory access.

Key Takeaways
Spark memory exists on both the driver and executors.
Executors perform most of the distributed data processing.
Executor memory contains JVM heap plus additional memory overhead.
Spark uses a unified memory model for execution and storage.
Execution memory is used for joins, aggregations, sorting, and related computation.
Storage memory is primarily used for caching and persistence.
Execution and storage can dynamically share memory.
cache() does not guarantee that all data permanently stays in RAM.
PySpark applications may require additional memory for Python workers.
When memory is insufficient, Spark may spill data to disk.
More executor memory does not automatically mean better performance.
Memory management should be considered together with partitions, parallelism, shuffle, joins, and data distribution.