# 04 — Spark Theory Guide

## Overview

This guide connects the major Spark concepts into one mental model.

Instead of memorizing individual terms like Driver, Executor, Job, Stage, Task, Partition, Shuffle, and Cluster Mode separately, the goal is to understand **how they are connected during Spark execution**.

The core Spark model is:

```text
Application
    ↓
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

And the execution hierarchy is:

```text
Application
    ↓
Jobs
    ↓
Stages
    ↓
Tasks
    ↓
Partitions
```

---

# 1. What Is Apache Spark?

Apache Spark is a distributed data processing engine.

It allows us to process large datasets by distributing computation across multiple machines.

Instead of:

```text
One Machine
    ↓
Process Entire Dataset
```

Spark can use:

```text
Cluster
│
├── Machine 1
├── Machine 2
├── Machine 3
└── Machine 4
```

The data is divided into partitions and processed in parallel.

---

# 2. The Most Important Spark Mental Model

Remember these three layers:

### Layer 1 — Application Coordination

```text
Driver
```

### Layer 2 — Resource Management

```text
Cluster Manager
```

### Layer 3 — Distributed Computation

```text
Executors
    ↓
Tasks
    ↓
Partitions
```

So:

```text
Driver
→ Coordinates

Cluster Manager
→ Allocates resources

Executors
→ Execute

Tasks
→ Process partitions
```

---

# 3. Spark Application

A Spark application is the complete program submitted to Spark.

Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("OrdersAnalysis") \
    .getOrCreate()

df = spark.read.parquet("s3://bucket/orders/")

result = (
    df.groupBy("customer_id")
      .sum("revenue")
)

result.show()

spark.stop()
```

This entire program is one Spark application.

---

# 4. Driver

The Driver is the coordinator of the Spark application.

Its major responsibilities include:

* Creating SparkSession
* Maintaining application state
* Building execution plans
* Creating jobs
* Dividing jobs into stages
* Creating tasks
* Scheduling tasks
* Tracking execution

Simple mental model:

```text
Driver = Brain
```

The Driver does not normally process the entire dataset itself.

---

# 5. SparkSession

`SparkSession` is the main entry point for modern Spark applications.

```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

Through SparkSession we interact with:

```text
DataFrame API
SQL
Spark configuration
Catalog
SparkContext
```

Relationship:

```text
Spark Application
       ↓
SparkSession
       ↓
Spark Engine
```

---

# 6. SparkContext

`SparkContext` is the lower-level interface responsible for connecting the application with the Spark execution environment.

Conceptually:

```text
SparkSession
     ↓
SparkContext
     ↓
Cluster Manager
```

Modern PySpark applications generally create `SparkSession` rather than manually creating `SparkContext`.

---

# 7. Cluster Manager

The Cluster Manager manages cluster resources.

Common examples include:

```text
Standalone
YARN
Kubernetes
```

Its responsibility is mainly:

```text
CPU
Memory
Executors
Resource Allocation
```

It does not perform the actual data processing.

---

# 8. Executors

Executors are processes that run on worker nodes.

They:

* Execute tasks
* Process partitions
* Store cached data
* Store intermediate data
* Report execution information to the Driver

Mental model:

```text
Executor = Worker Process
```

Example:

```text
Executor
│
├── Task
├── Task
├── Task
└── Task
```

---

# 9. Worker Node vs Executor

These are not the same.

### Worker Node

A machine that provides resources.

### Executor

A process running on a worker node.

Conceptually:

```text
Worker Node
│
└── Executor
```

One worker node can host executor processes depending on the deployment configuration.

---

# 10. Driver vs Executor

| Driver                      | Executor                         |
| --------------------------- | -------------------------------- |
| Coordinates application     | Executes tasks                   |
| Builds execution plan       | Processes partitions             |
| Schedules tasks             | Runs tasks                       |
| Tracks execution            | Stores intermediate data         |
| Usually one per application | Usually multiple                 |
| Can become a bottleneck     | Performs distributed computation |

Remember:

```text
Driver
→ Plans and coordinates

Executors
→ Execute and process
```

---

# 11. Lazy Evaluation

Spark transformations are lazy.

Example:

```python
filtered_df = df.filter(
    df["amount"] > 100
)
```

Spark does not immediately execute the filter.

Instead, Spark records the transformation as part of the execution plan.

```text
Transformation
      ↓
Logical Plan
      ↓
No immediate execution
```

---

# 12. Actions

Actions trigger execution.

Examples:

```python
df.show()
df.count()
df.collect()
df.write.parquet(...)
```

The basic relationship is:

```text
Transformation
    ↓
Transformation
    ↓
Action
    ↓
Job
```

---

# 13. Why Lazy Evaluation?

Lazy evaluation allows Spark to optimize the complete computation before execution.

Instead of executing each transformation separately:

```text
Filter → Execute
Select → Execute
GroupBy → Execute
```

Spark can build and optimize the pipeline:

```text
Filter
   ↓
Select
   ↓
GroupBy
   ↓
Optimize
   ↓
Execute
```

This can reduce unnecessary computation and data movement.

---

# 14. Job

A Spark Job is generally created when an action triggers execution.

Example:

```python
df.count()
```

can create a job.

Another action:

```python
df.show()
```

can create another job.

Therefore:

```text
One Application
    ↓
Can contain multiple Jobs
```

---

# 15. Stage

A job is divided into stages.

Stages represent portions of execution separated by dependency boundaries, especially shuffle boundaries.

Example:

```text
Job
│
├── Stage 0
│
├── Stage 1
│
└── Stage 2
```

The key idea:

> A shuffle often creates a stage boundary.

---

# 16. Task

A stage is divided into tasks.

A useful mental model is:

```text
One Task
    ↓
Processes One Partition
```

Example:

```text
Stage
│
├── Task 0 → Partition 0
├── Task 1 → Partition 1
├── Task 2 → Partition 2
└── Task 3 → Partition 3
```

Tasks are the units of work actually executed by executors.

---

# 17. Partition

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

Spark processes partitions independently whenever possible.

Therefore:

```text
Partition
    ↓
Task
    ↓
Executor
```

---

# 18. Application → Job → Stage → Task

This hierarchy should be memorized:

```text
Application
│
├── Job
│   ├── Stage
│   │   ├── Task
│   │   ├── Task
│   │   └── Task
│   │
│   └── Stage
│       ├── Task
│       └── Task
│
└── Job
    └── Stage
        ├── Task
        └── Task
```

Simplified:

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

---

# 19. Narrow Transformation

A narrow transformation generally does not require major redistribution of data.

Examples:

```text
filter
select
withColumn
map
```

Conceptually:

```text
Partition 0 → Partition 0
Partition 1 → Partition 1
Partition 2 → Partition 2
```

Data remains relatively local.

---

# 20. Wide Transformation

A wide transformation requires data redistribution.

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

Wide transformations are generally more expensive because of shuffle.

---

# 21. Shuffle

Shuffle is the redistribution of data across partitions.

Example:

```python
df.groupBy("customer_id").count()
```

Records with the same key need to be brought together.

Initially:

```text
Partition 0 → Customer A, B
Partition 1 → Customer A, C
Partition 2 → Customer B, C
```

After shuffle:

```text
Partition 0 → Customer A
Partition 1 → Customer B
Partition 2 → Customer C
```

The exact partition assignment depends on Spark's partitioning strategy.

---

# 22. Why Shuffle Is Expensive

Shuffle can involve:

```text
Network I/O
Disk I/O
Serialization
Memory usage
CPU
```

Conceptually:

```text
Executor 1 ─┐
Executor 2 ─┼──► Network ──► Executors
Executor 3 ─┘
```

Therefore, excessive shuffle can significantly affect performance.

---

# 23. Stage Boundary

A simplified execution graph:

```text
Stage 0
│
├── Read
├── Filter
└── Map
      │
      ▼
   Shuffle
      │
      ▼
Stage 1
│
├── Aggregate
└── Output
```

The shuffle separates the stages.

---

# 24. DAG

DAG stands for:

> Directed Acyclic Graph

Spark represents transformations as a graph.

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

Spark uses this graph to determine how the application should be executed.

---

# 25. Logical Plan

For DataFrame and SQL operations, Spark first represents the query logically.

Example:

```python
df.filter("amount > 100") \
  .select("customer_id", "amount")
```

Conceptually:

```text
Logical Plan

Filter
  ↓
Project
```

The logical plan describes **what** needs to be done rather than the exact low-level execution steps.

---

# 26. Catalyst Optimizer

Spark SQL uses the Catalyst optimizer.

It can perform optimizations such as:

```text
Predicate Pushdown
Column Pruning
Constant Folding
Expression Simplification
Join Optimizations
```

For example:

```python
df.filter(df["age"] > 30) \
  .select("name")
```

Spark can avoid processing unnecessary columns or rows where possible.

---

# 27. Physical Plan

The physical plan describes **how** Spark intends to execute the query.

Use:

```python
df.explain()
```

or:

```python
df.explain("formatted")
```

You may see operators such as:

```text
Scan
Filter
Project
Exchange
HashAggregate
Sort
Join
```

---

# 28. `Exchange`

When you see:

```text
Exchange
```

in a physical plan, it commonly represents a data redistribution boundary.

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

---

# 29. Parallelism

Spark achieves parallelism by processing multiple partitions simultaneously.

Suppose:

```text
100 partitions
```

and:

```text
20 available task slots
```

Spark may process approximately:

```text
20 tasks
20 tasks
20 tasks
20 tasks
20 tasks
```

in successive waves.

Therefore:

```text
Parallelism
≈
Available Task Slots
```

but actual execution depends on scheduling, resources, dependencies, and configuration.

---

# 30. Executor Cores

Suppose:

```text
3 Executors
4 Cores per Executor
```

Potential concurrent task capacity:

```text
3 × 4 = 12 task slots
```

Conceptually:

```text
Executor 1
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task

Executor 2
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task

Executor 3
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task
```

This is a simplified model; actual scheduling depends on Spark configuration and the deployment environment.

---

# 31. Number of Tasks

A useful rule:

```text
Tasks in a stage
≈
Number of partitions processed by that stage
```

Example:

```text
Stage
│
├── Partition 0 → Task 0
├── Partition 1 → Task 1
├── Partition 2 → Task 2
└── Partition 3 → Task 3
```

So if a stage processes 4 partitions, Spark generally creates 4 tasks for that stage.

---

# 32. Too Few Partitions

Suppose:

```text
4 partitions
100 available task slots
```

Only around 4 tasks can run for that stage.

Many resources remain unused.

```text
100 task slots
     ↑
Only 4 tasks
```

This can result in poor cluster utilization.

---

# 33. Too Many Partitions

Now suppose:

```text
1,000,000 tiny partitions
```

There may be excessive scheduling overhead.

Spark has to manage a huge number of tasks.

Therefore:

```text
Too Few Partitions
→ Poor parallelism

Too Many Tiny Partitions
→ Scheduling overhead
```

The goal is balanced partitioning.

---

# 34. Data Skew

Data skew occurs when some partitions contain significantly more data than others.

Example:

```text
Partition 0 → 10 MB
Partition 1 → 12 MB
Partition 2 → 11 MB
Partition 3 → 2 GB
```

Then:

```text
Task 0 → Fast
Task 1 → Fast
Task 2 → Fast
Task 3 → Very Slow
```

The entire stage may wait for the slow task.

This is often called a **straggler task**.

---

# 35. Data Locality

Spark tries to process data close to where it is stored when possible.

Conceptually:

```text
Data
  ↓
Nearest Executor
  ↓
Task
```

Moving data over the network can be expensive, so locality can improve performance.

---

# 36. Fault Tolerance

Spark can recover from certain failures.

If a task fails:

```text
Task Failed
     ↓
Driver detects failure
     ↓
Task rescheduled
     ↓
Another executor
     ↓
Task re-executed
```

Spark also uses lineage information to recompute lost intermediate data when necessary.

---

# 37. Lineage

Lineage describes how a dataset was produced.

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

If an intermediate partition is lost, Spark can use this information to recompute it.

This is an important part of Spark's fault-tolerance model.

---

# 38. Cache and Persist

Spark can keep computed data for reuse.

Example:

```python
df.cache()
```

or:

```python
df.persist()
```

Conceptually:

```text
First Action
    ↓
Compute Data
    ↓
Cache
    ↓
Second Action
    ↓
Reuse Cached Data
```

Caching is useful when the same DataFrame/RDD is reused multiple times.

Do not cache everything.

Caching itself consumes resources.

---

# 39. Driver Memory Problem

A common mistake is bringing too much data to the Driver.

For example:

```python
df.collect()
```

or:

```python
df.toPandas()
```

Conceptually:

```text
Executors
   │
   │ Large Result
   ▼
 Driver
   │
   ▼
OutOfMemoryError
```

Distributed processing does not help if we intentionally collect the entire dataset to one machine.

---

# 40. Driver vs Executor Memory

### Driver Memory

Used for things such as:

```text
Application coordination
Execution planning
Driver-side objects
Metadata
Collected results
```

### Executor Memory

Used for:

```text
Task execution
Cached data
Shuffle data
Intermediate computation
```

Therefore:

```text
Driver Memory
≠
Executor Memory
```

---

# 41. Cluster Mode

Spark can run in:

```text
Client Mode
Cluster Mode
```

The main difference:

### Client Mode

```text
Driver
↓
Submission machine
```

### Cluster Mode

```text
Driver
↓
Inside cluster
```

Executors run in the cluster in both cases.

---

# 42. Client Mode

```text
Client Machine
│
└── Driver
      │
      ▼
    Cluster
    ├── Executor
    ├── Executor
    └── Executor
```

Useful for:

```text
Interactive development
Debugging
Testing
```

---

# 43. Cluster Mode

```text
Client
│
└── spark-submit
       │
       ▼
    Cluster
    ├── Driver
    ├── Executor
    ├── Executor
    └── Executor
```

Useful for:

```text
Production
Scheduled jobs
Automated pipelines
```

---

# 44. `--master` vs `--deploy-mode`

This is a common interview question.

### `--master`

Specifies the cluster manager / execution environment.

Example:

```bash
--master yarn
```

### `--deploy-mode`

Specifies where the Driver runs.

Example:

```bash
--deploy-mode cluster
```

Remember:

```text
--master
→ Which resource manager?

--deploy-mode
→ Where does Driver run?
```

---

# 45. Spark on EMR

A simplified EMR architecture:

```text
                    EMR Cluster
                         │
              ┌──────────┴──────────┐
              │                     │
        Primary Node          Core / Task Nodes
              │                     │
           Driver               Executors
              │                     │
              └──────────┬──────────┘
                         │
                         ▼
                        S3
```

When working with EMR, understand:

```text
EMR
↓
Cluster
↓
Spark
↓
Driver + Executors
```

---

# 46. Spark UI

Spark UI helps us understand actual execution.

Important sections include:

```text
Jobs
Stages
Storage
Environment
Executors
SQL
```

Use Spark UI to investigate:

```text
Slow stages
Failed tasks
Shuffle
Data skew
Executor memory
Spill
Task duration
```

---

# 47. Reading Spark UI

Suppose one stage shows:

```text
Task 0 → 2 sec
Task 1 → 2 sec
Task 2 → 3 sec
Task 3 → 180 sec
```

This is suspicious.

Possible reason:

```text
Data Skew
```

One partition may contain significantly more data than the others.

---

# 48. Reading Physical Plans

Suppose:

```text
df.groupBy("customer_id").count()
```

and `explain()` shows:

```text
Exchange
   ↓
HashAggregate
```

Think:

```text
Exchange
    ↓
Shuffle
    ↓
Aggregation
```

If you see:

```text
BroadcastHashJoin
```

Spark may be using a broadcast join strategy.

If you see:

```text
SortMergeJoin
```

Spark is using a sort-merge based join strategy.

---

# 49. End-to-End Example

Consider:

```python
result = (
    orders_df
    .filter(F.col("quantity") > 5)
    .groupBy("customer_id")
    .agg(
        F.sum(
            F.col("unit_price") * F.col("quantity")
        ).alias("revenue")
    )
)

result.show()
```

Execution conceptually looks like:

```text
Application
     ↓
Driver
     ↓
Action: show()
     ↓
Job
     ↓
Stage 0
     │
     ├── Read
     └── Filter
     │
     ▼
  Shuffle
     │
     ▼
Stage 1
     │
     ├── Aggregate
     └── Produce Result
     │
     ▼
Executors
     ↓
Tasks
     ↓
Partitions
```

---

# 50. Another Example — Join

```python
result = orders_df.join(
    customers_df,
    "customer_id"
)
```

Possible execution:

```text
orders
  ↓
Shuffle ───┐
           │
           ├── Join
           │
customers ─┘
  ↓
Shuffle
```

Or Spark may choose a broadcast join:

```text
Small Table
    ↓
Broadcast
    ↓
Executors
    ↓
Join with Large Table
```

The actual strategy depends on statistics, configuration, and the physical plan.

---

# 51. Performance Thinking

When a Spark job is slow, do not immediately increase:

```text
Executors
Memory
CPU
```

First ask:

```text
1. Is there a shuffle?
2. Is there data skew?
3. Are partitions balanced?
4. Are there too many/few partitions?
5. Is there unnecessary data movement?
6. Is a join strategy appropriate?
7. Is the Driver collecting too much data?
8. Is there spilling?
9. Is the input format efficient?
10. Is the cluster actually being utilized?
```

---

# 52. Spark Performance Mental Model

A useful performance equation is:

```text
Spark Performance
        ↓
Data
        +
Partitioning
        +
Parallelism
        +
CPU
        +
Memory
        +
Network
        +
Disk I/O
        +
Shuffle
        +
Query Plan
```

Performance is not controlled by a single configuration.

---

# 53. Common Misconceptions

### Misconception 1

> Every transformation creates a job.

Wrong.

```text
Transformations
→ Lazy

Actions
→ Trigger execution
```

---

### Misconception 2

> Every transformation creates a stage.

Wrong.

Stage boundaries are associated with dependencies and shuffle.

---

### Misconception 3

> One stage equals one task.

Wrong.

A stage normally contains multiple tasks.

---

### Misconception 4

> One task processes the entire DataFrame.

Wrong.

A task generally processes one partition.

---

### Misconception 5

> Executor and worker node are the same.

Wrong.

```text
Worker Node
→ Machine

Executor
→ Process
```

---

### Misconception 6

> More executors always make Spark faster.

Wrong.

You can still have:

```text
Data Skew
Too Few Partitions
Shuffle Bottleneck
I/O Bottleneck
Driver Bottleneck
```

---

### Misconception 7

> Cluster mode means executors run on the Driver.

Wrong.

The Driver coordinates; Executors execute.

---

# 54. Golden Rules

Keep these rules in mind.

### Rule 1

```text
Action → Job
```

### Rule 2

```text
Job → Stages
```

### Rule 3

```text
Stage → Tasks
```

### Rule 4

```text
Task → Partition
```

### Rule 5

```text
Wide Dependency → Shuffle
```

### Rule 6

```text
Shuffle → Usually Stage Boundary
```

### Rule 7

```text
Driver → Coordinates
```

### Rule 8

```text
Executor → Executes
```

### Rule 9

```text
Cluster Manager → Allocates Resources
```

### Rule 10

```text
Client Mode → Driver on Submission Machine
Cluster Mode → Driver Inside Cluster
```

---

# 55. One-Page Mental Model

If you remember only one diagram from this guide, remember this:

```text
                    SPARK APPLICATION
                           │
                           ▼
                        DRIVER
                           │
                ┌──────────┴──────────┐
                │                     │
         Builds Execution       Schedules Work
             Plan                   │
                │                   ▼
                │                  JOB
                │                   │
                │                STAGES
                │                   │
                │                 TASKS
                │                   │
                ▼                   ▼
          CLUSTER MANAGER      PARTITIONS
                │
                ▼
           RESOURCES
                │
        ┌───────┼───────┐
        ▼       ▼       ▼
    EXECUTOR EXECUTOR EXECUTOR
        │       │       │
      Tasks   Tasks   Tasks
        │       │       │
   Partitions Partitions Partitions
```

---

# 56. Final Interview Cheat Sheet

| Concept           | Remember                        |
| ----------------- | ------------------------------- |
| Spark Application | Complete Spark program          |
| Driver            | Coordinates application         |
| SparkSession      | Main entry point                |
| SparkContext      | Lower-level Spark interface     |
| Cluster Manager   | Allocates resources             |
| Worker Node       | Machine                         |
| Executor          | Process running on worker       |
| Transformation    | Lazy operation                  |
| Action            | Triggers execution              |
| Job               | Created by action               |
| Stage             | Portion of a job                |
| Task              | Unit of execution               |
| Partition         | Unit of distributed data        |
| Narrow            | Limited dependency              |
| Wide              | Requires redistribution         |
| Shuffle           | Data redistribution             |
| DAG               | Execution dependency graph      |
| Catalyst          | Spark SQL optimizer             |
| Physical Plan     | How Spark executes              |
| Exchange          | Redistribution/shuffle operator |
| Cache             | Reuse computed data             |
| Lineage           | Transformation history          |
| Client Mode       | Driver on submission machine    |
| Cluster Mode      | Driver inside cluster           |
| Spark UI          | Monitoring and debugging        |

---

# 57. Final Mental Model

The complete Spark story can be summarized in one sentence:

> **A Spark application is coordinated by the Driver, resources are provided by the Cluster Manager, Executors run tasks against partitions, actions trigger jobs, jobs are divided into stages, shuffle creates dependency boundaries, and Spark executes the resulting tasks in parallel across the cluster.**

```text
Application
     ↓
Driver
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

This is the foundation for understanding Spark performance, memory management, joins, shuffle, partitioning, and cluster tuning.
