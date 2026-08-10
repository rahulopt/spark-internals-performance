# 02 — Spark Architecture

## Overview

Apache Spark is a distributed data processing engine designed to process large datasets across multiple machines.

To understand Spark properly, we need to understand how these components work together:

```text
Spark Application
        │
        ▼
      Driver
        │
        ▼
 Cluster Manager
        │
        ▼
    Executors
        │
        ▼
      Tasks
        │
        ▼
   Partitions
```

The architecture becomes much easier to understand when we follow the lifecycle of a Spark application from submission to completion.

---

# 1. High-Level Spark Architecture

A Spark application consists of several major components:

```text
                         Spark Application
                                │
                        ┌───────┴───────┐
                        │               │
                     Driver        Cluster Manager
                        │               │
                        │         Resource Allocation
                        │               │
                        └───────┬───────┘
                                │
                         Executor Processes
                         ┌──────┼──────┐
                         │      │      │
                         ▼      ▼      ▼
                       Exec1  Exec2  Exec3
                         │      │      │
                       Tasks  Tasks  Tasks
                         │      │      │
                    Partitions Partitions
```

The important distinction is:

> **The Driver coordinates the application, while Executors perform the distributed computation.**

---

# 2. What Is a Spark Application?

A Spark application is a program written using Spark APIs.

For example:

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

This complete program represents a Spark application.

When submitted, Spark creates:

```text
Application
    │
    ├── Driver
    └── Executors
```

---

# 3. The Driver

The **Driver** is the central coordinator of a Spark application.

It is responsible for:

* Creating the Spark application
* Creating `SparkSession`
* Maintaining application state
* Building execution plans
* Scheduling jobs
* Dividing jobs into stages
* Creating tasks
* Sending tasks to executors
* Tracking task progress

Conceptually:

```text
Driver
│
├── SparkSession
├── DAG / Execution Plan
├── Job Scheduling
├── Stage Scheduling
└── Task Scheduling
```

The driver is therefore the **brain of the Spark application**.

---

# 4. SparkSession

In modern Spark applications, `SparkSession` is the main entry point.

Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApplication") \
    .getOrCreate()
```

It provides access to Spark functionality such as:

```text
DataFrame API
SQL
SparkContext
Configuration
Catalog
```

Conceptually:

```text
Spark Application
       │
       ▼
SparkSession
       │
       ▼
Spark Engine
```

---

# 5. SparkContext

`SparkContext` is the lower-level entry point to Spark's core execution functionality.

In modern applications, we usually create:

```python
SparkSession
```

instead of manually creating a `SparkContext`.

The relationship can be viewed as:

```text
SparkSession
     │
     └── SparkContext
```

`SparkContext` communicates with the cluster manager and coordinates the underlying Spark execution environment.

---

# 6. Cluster Manager

The cluster manager is responsible for allocating resources to Spark applications.

Spark can work with different cluster managers, such as:

```text
Standalone
YARN
Kubernetes
```

Cloud platforms can also provide managed Spark environments, such as Amazon EMR.

Conceptually:

```text
Driver
   │
   │ Request Resources
   ▼
Cluster Manager
   │
   ├── CPU
   ├── Memory
   └── Executors
```

The cluster manager does **not** perform the actual Spark data processing.

Its main responsibility is resource management.

---

# 7. Executors

Executors are processes that run on worker nodes.

They perform the actual distributed computation.

Executors are responsible for:

* Running tasks
* Processing partitions
* Storing intermediate data
* Storing cached data
* Reporting task results and metrics to the driver

Conceptually:

```text
Executor
│
├── Task 1
├── Task 2
├── Task 3
└── Task 4
```

The number of concurrent tasks depends on executor resources and configuration.

---

# 8. Driver vs Executor

This is one of the most important Spark interview concepts.

| Driver                      | Executor                   |
| --------------------------- | -------------------------- |
| Coordinates application     | Executes tasks             |
| Creates execution plan      | Processes partitions       |
| Schedules jobs/stages/tasks | Runs tasks                 |
| Tracks execution            | Stores intermediate data   |
| Maintains application state | Can cache data             |
| Usually one driver          | Usually multiple executors |

Simple mental model:

```text
Driver
= Brain

Executors
= Workers
```

---

# 9. Worker Nodes

Worker nodes are machines that provide resources for executing Spark tasks.

Depending on the cluster manager, the exact terminology can differ.

Conceptually:

```text
Cluster
│
├── Driver Node
│
├── Worker Node 1
│   └── Executor
│
├── Worker Node 2
│   └── Executor
│
└── Worker Node 3
    └── Executor
```

An executor is a process running on a worker node.

Therefore:

> **Worker node and executor are not the same thing.**

A worker node is a machine/resource host.

An executor is a process running there.

---

# 10. Application Lifecycle

Let's follow a Spark application from beginning to end.

```text
Submit Application
       ↓
Driver Starts
       ↓
SparkSession Created
       ↓
Cluster Resources Requested
       ↓
Executors Start
       ↓
Application Builds Logical Plan
       ↓
Action Triggered
       ↓
Job Created
       ↓
Stages Created
       ↓
Tasks Created
       ↓
Tasks Sent to Executors
       ↓
Executors Process Partitions
       ↓
Results Returned / Written
       ↓
Application Completes
```

This is the basic Spark execution lifecycle.

---

# 11. Lazy Evaluation

Spark transformations are lazy.

For example:

```python
df2 = df.filter("amount > 100")
```

Spark does not immediately execute the filtering operation.

Instead, Spark records the transformation.

Conceptually:

```text
filter()
   ↓
No execution yet
   ↓
Transformation recorded
```

Execution begins when an action is called.

For example:

```python
df2.count()
```

Now:

```text
Action
  ↓
Job
  ↓
Stages
  ↓
Tasks
  ↓
Execution
```

---

# 12. Transformations

Transformations create a new logical dataset.

Examples:

```python
df.filter(...)
df.select(...)
df.withColumn(...)
df.join(...)
df.groupBy(...)
```

Transformations are generally lazy.

Example:

```python
filtered_df = df.filter(
    df["amount"] > 100
)
```

No Spark job is necessarily executed at this point.

---

# 13. Actions

Actions trigger execution.

Examples:

```python
df.show()
df.count()
df.collect()
df.write.parquet(...)
```

For example:

```python
df.filter(
    df["amount"] > 100
).count()
```

The `count()` action triggers Spark to execute the required computation.

---

# 14. Job

A **job** is created when an action triggers execution.

Example:

```python
df.count()
```

can create a Spark job.

Another action:

```python
df.show()
```

can create another job.

Conceptually:

```text
Action
  ↓
Job
```

A single Spark application can contain multiple jobs.

---

# 15. Stage

A job is divided into one or more stages.

Stages are boundaries in Spark's execution plan, especially around shuffle operations.

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

The major reason stages exist is to separate portions of execution that can be processed together from portions that require data redistribution.

---

# 16. Shuffle Boundary

Shuffle is one of the most important concepts in Spark architecture.

Suppose:

```python
df.groupBy("customer_id").count()
```

The data needs to be grouped by `customer_id`.

Records with the same key may initially exist in different partitions.

Spark must redistribute them.

Conceptually:

```text
Partition 1 ─┐
Partition 2 ─┼──► Shuffle ──► New Partitions
Partition 3 ─┤
Partition 4 ─┘
```

This redistribution creates a stage boundary.

---

# 17. Tasks

A stage is divided into tasks.

The most useful mental model is:

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

These tasks can run in parallel if resources are available.

---

# 18. Partitions

A Spark DataFrame/RDD is logically divided into partitions.

For example:

```text
Dataset
│
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

Spark uses partitions as the basic unit of parallel processing.

Therefore:

```text
Partition
    ↓
Task
    ↓
Executor
```

This relationship is extremely important.

---

# 19. The Task–Partition Relationship

Suppose a stage has:

```text
4 partitions
```

Spark can create approximately:

```text
4 tasks
```

for that stage.

Conceptually:

```text
Partition 0 → Task 0
Partition 1 → Task 1
Partition 2 → Task 2
Partition 3 → Task 3
```

If the cluster has enough available cores, these tasks can run concurrently.

---

# 20. Parallelism

Suppose:

```text
100 partitions
```

and the cluster has:

```text
20 available task slots
```

Spark cannot execute all 100 tasks simultaneously.

It runs them in waves:

```text
Wave 1 → 20 tasks
Wave 2 → 20 tasks
Wave 3 → 20 tasks
Wave 4 → 20 tasks
Wave 5 → 20 tasks
```

Therefore:

> Number of partitions and available task slots together influence parallelism.

---

# 21. Executor Cores

Suppose an executor has:

```text
4 cores
```

It can potentially run around:

```text
4 tasks concurrently
```

Conceptually:

```text
Executor
│
├── Core 1 → Task
├── Core 2 → Task
├── Core 3 → Task
└── Core 4 → Task
```

If there are multiple executors:

```text
Executor 1 → 4 tasks
Executor 2 → 4 tasks
Executor 3 → 4 tasks
```

Total available task slots:

```text
4 × 3 = 12
```

assuming the resources are fully available and the deployment configuration allows it.

---

# 22. Complete Execution Hierarchy

Memorize this hierarchy:

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

But remember:

> A task is the unit that processes a partition.

And:

```text
Driver
    ↓
Schedules
    ↓
Jobs
    ↓
Stages
    ↓
Tasks
    ↓
Executors execute tasks
```

---

# 23. DAG

DAG stands for:

> **Directed Acyclic Graph**

Spark builds a DAG representing the transformations required to produce the final result.

Example:

```python
df2 = df.filter("amount > 100")

df3 = df2.select(
    "customer_id",
    "amount"
)

result = df3.groupBy(
    "customer_id"
).sum("amount")

result.show()
```

Conceptually:

```text
Read Data
    ↓
Filter
    ↓
Select
    ↓
GroupBy
    ↓
Action
```

Spark analyzes this graph to create an execution plan.

---

# 24. Narrow Transformation

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
Parent Partition 0 ──► Child Partition 0
Parent Partition 1 ──► Child Partition 1
Parent Partition 2 ──► Child Partition 2
```

No major redistribution of data is required.

---

# 25. Wide Transformation

A wide transformation requires data to be redistributed across partitions.

Examples include:

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
P1 ─┼──► Shuffle ──► P0'
P2 ─┤                 P1'
P3 ─┘                 P2'
                      P3'
```

Wide transformations usually introduce shuffle and stage boundaries.

---

# 26. Narrow vs Wide

| Narrow                    | Wide                    |
| ------------------------- | ----------------------- |
| Limited parent dependency | Many-to-many dependency |
| No major shuffle          | Shuffle required        |
| Usually cheaper           | Usually more expensive  |
| Filter                    | GroupBy                 |
| Select                    | Join                    |
| WithColumn                | Distinct                |
| Map                       | OrderBy                 |

Simple rule:

```text
Narrow
→ Data stays relatively local

Wide
→ Data moves across partitions
```

---

# 27. Example: Narrow Transformation

```python
filtered_df = df.filter(
    df["quantity"] > 10
)
```

Conceptually:

```text
Partition 0 → Filter → Partition 0
Partition 1 → Filter → Partition 1
Partition 2 → Filter → Partition 2
```

Each partition can be processed independently.

---

# 28. Example: Wide Transformation

```python
result = df.groupBy(
    "customer_id"
).sum("revenue")
```

Spark must bring records with the same `customer_id` together.

Therefore:

```text
Original Partitions
        ↓
      Shuffle
        ↓
Grouped Partitions
```

This creates a stage boundary.

---

# 29. Stage Execution Example

Consider:

```python
result = (
    df.filter(df["amount"] > 100)
      .groupBy("customer_id")
      .sum("amount")
)

result.show()
```

Conceptually:

```text
Stage 0
│
├── Read
├── Filter
└── Prepare Shuffle
        │
        ▼
      Shuffle
        │
        ▼
Stage 1
│
├── Read Shuffle Data
├── Aggregate
└── Produce Result
```

The exact physical plan can vary depending on Spark's optimizer and execution strategy.

---

# 30. SQL/DataFrame Query Planning

Spark SQL/DataFrame operations go through multiple planning stages.

A simplified view is:

```text
User Code
    ↓
Unresolved Logical Plan
    ↓
Analyzed Logical Plan
    ↓
Optimized Logical Plan
    ↓
Physical Plan
    ↓
RDD / Execution
```

This is largely handled by Spark SQL's Catalyst optimizer and execution engine.

---

# 31. Catalyst Optimizer

Catalyst is Spark SQL's query optimization framework.

It can apply optimizations such as:

* Predicate pushdown
* Column pruning
* Constant folding
* Expression simplification
* Join-related optimizations
* Other logical and physical optimizations

Example:

```python
df.filter(df["age"] > 30) \
  .select("name")
```

Spark can optimize the execution so that unnecessary columns and data are not processed.

---

# 32. Physical Plan

The physical plan describes how Spark intends to execute the query.

We can inspect it using:

```python
df.explain()
```

or:

```python
df.explain("formatted")
```

Example:

```text
== Physical Plan ==
...
```

The physical plan can reveal:

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

# 33. `Exchange`

When you see:

```text
Exchange
```

in a physical plan, it often indicates a redistribution/shuffle boundary.

For example:

```text
Filter
   ↓
Exchange
   ↓
HashAggregate
```

The `Exchange` represents data redistribution between partitions.

This is a useful Spark performance clue.

---

# 34. Driver Scheduling

The driver coordinates the execution.

Conceptually:

```text
Driver
  │
  ├── Job
  │    ├── Stage
  │    │    ├── Task
  │    │    ├── Task
  │    │    └── Task
  │    │
  │    └── Stage
  │
  └── Job
```

The driver determines which tasks need to be launched and tracks their execution.

---

# 35. Executor Communication

Executors communicate with the driver to report:

```text
Task completion
Task failure
Metrics
Status
Results / metadata
```

Conceptually:

```text
Driver
  │
  ├──── Task ────► Executor 1
  │
  ├──── Task ────► Executor 2
  │
  └──── Task ────► Executor 3
```

Executors then report status back to the driver.

---

# 36. Fault Tolerance

Spark is designed to recover from certain failures.

For example, if a task fails:

```text
Task Failed
    ↓
Driver Detects Failure
    ↓
Task Rescheduled
    ↓
Another Executor
    ↓
Task Re-executed
```

Spark's lineage information can also help recompute lost data.

This is one of the major reasons Spark can operate reliably on distributed clusters.

---

# 37. Lineage

Spark maintains information about how a dataset was created.

For example:

```text
Source
  ↓
Filter
  ↓
Select
  ↓
GroupBy
```

If intermediate data is lost, Spark can potentially recompute it from the original source and transformations.

This is called **lineage**.

---

# 38. Why Spark Uses Lazy Evaluation

Lazy evaluation allows Spark to analyze the complete transformation pipeline before executing it.

Instead of:

```text
Execute filter
   ↓
Execute select
   ↓
Execute groupBy
```

Spark can reason about the complete pipeline:

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

This enables query optimization and efficient execution planning.

---

# 39. Spark UI

Spark provides a web UI for monitoring applications.

Important sections include:

```text
Jobs
Stages
Storage
Environment
Executors
SQL
```

The UI helps answer questions such as:

```text
Which stage is slow?
Which task is slow?
How much data was shuffled?
Which executor failed?
How much memory is being used?
How much data was spilled?
```

---

# 40. Application → Job → Stage → Task

This hierarchy is one of the most important things to understand.

```text
Application
│
├── Job 0
│   ├── Stage 0
│   │   ├── Task 0
│   │   ├── Task 1
│   │   └── Task 2
│   │
│   └── Stage 1
│       ├── Task 0
│       ├── Task 1
│       └── Task 2
│
└── Job 1
    └── Stage 2
        ├── Task 0
        ├── Task 1
        └── Task 2
```

Remember:

```text
Application
   contains Jobs

Job
   contains Stages

Stage
   contains Tasks

Task
   processes a Partition
```

---

# 41. A Complete Example

Consider:

```python
from pyspark.sql import functions as F

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

Let's break this down.

### Step 1 — Read

```text
orders_df
```

Spark reads the source data.

---

### Step 2 — Filter

```python
.filter(...)
```

This is generally a narrow operation.

---

### Step 3 — GroupBy

```python
.groupBy("customer_id")
```

This requires data redistribution.

Therefore, a shuffle may occur.

---

### Step 4 — Aggregation

```python
.agg(...)
```

Spark aggregates data after the shuffle.

---

### Step 5 — Action

```python
.show()
```

The action triggers execution.

---

### Overall Flow

```text
Read
 ↓
Filter
 ↓
Shuffle
 ↓
Aggregate
 ↓
Show
```

---

# 42. Another Example: Join

Consider:

```python
result = orders_df.join(
    customers_df,
    "customer_id"
)
```

Depending on the join strategy, Spark may perform:

```text
orders
   ↓
Shuffle ─────┐
             ├── Join
customers    │
   ↓         │
Shuffle ─────┘
```

Or Spark may choose a broadcast strategy:

```text
Small customers
       ↓
Broadcast
       ↓
Executors
       ↓
Join with orders
```

The physical plan determines the actual execution strategy.

---

# 43. Why Spark Architecture Matters

Understanding architecture helps us answer performance questions.

For example:

### Why is my job slow?

Look at:

```text
Stages
Tasks
Shuffle
Partitions
Skew
```

### Why did one task fail?

Look at:

```text
Partition size
Executor resources
Data skew
Memory
```

### Why is the driver out of memory?

Look for:

```text
collect()
toPandas()
Large driver-side objects
```

### Why is there a lot of network traffic?

Look for:

```text
Shuffle
Join
GroupBy
Repartition
```

---

# 44. Common Architecture Misconceptions

## Misconception 1

> "Driver processes all the data."

Not normally.

The driver coordinates execution while executors process partitions.

---

## Misconception 2

> "Executor = Worker Node."

Not exactly.

```text
Worker Node
    ↓
Machine

Executor
    ↓
Process running on that machine
```

---

## Misconception 3

> "Every transformation creates a job."

No.

Transformations are lazy.

Actions trigger jobs.

---

## Misconception 4

> "Every transformation creates a stage."

No.

Stages are primarily separated by dependencies such as shuffle boundaries.

---

## Misconception 5

> "One stage means one task."

No.

A stage can contain many tasks.

Generally:

```text
Tasks ≈ Number of partitions processed by that stage
```

---

## Misconception 6

> "More executors always make Spark faster."

Not necessarily.

Performance depends on:

```text
Partitioning
Parallelism
CPU
Memory
Shuffle
Network
Data Skew
I/O
```

---

# 45. Complete Spark Architecture Diagram

```text
                         Spark Application
                                │
                                ▼
                             Driver
                                │
                    ┌───────────┼───────────┐
                    │           │           │
              SparkSession    DAG      Scheduler
                    │                       │
                    │                 Cluster Manager
                    │                       │
                    │              Resource Allocation
                    │                       │
                    └───────────────┬───────┘
                                    │
                         ┌──────────┼──────────┐
                         │          │          │
                      Executor   Executor   Executor
                         │          │          │
                       Tasks      Tasks      Tasks
                         │          │          │
                    Partitions  Partitions  Partitions
                         │          │          │
                         └──────────┼──────────┘
                                    │
                                  Data
```

---

# 46. End-to-End Execution Flow

The complete mental model is:

```text
1. User writes Spark application
                ↓
2. Application submitted
                ↓
3. Driver starts
                ↓
4. SparkSession / SparkContext initialized
                ↓
5. Cluster resources requested
                ↓
6. Executors launched
                ↓
7. User transformations build a logical plan
                ↓
8. Action triggers execution
                ↓
9. Driver creates a Job
                ↓
10. Job is divided into Stages
                ↓
11. Stages are divided into Tasks
                ↓
12. Tasks process Partitions
                ↓
13. Executors execute tasks
                ↓
14. Shuffle occurs where required
                ↓
15. Results are produced
                ↓
16. Application completes
```

---

# 47. The Most Important Mental Model

Remember these five relationships:

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

And:

```text
Driver
  ↓
Coordinates

Executors
  ↓
Execute

Cluster Manager
  ↓
Allocates Resources
```

And:

```text
Narrow Transformation
  ↓
No major shuffle

Wide Transformation
  ↓
Shuffle
  ↓
Stage Boundary
```

---

# 48. Interview Answer: Explain Spark Architecture

A strong interview answer:

> Spark follows a driver-executor architecture. A Spark application has a driver that coordinates the application, builds the execution plan, and schedules jobs, stages, and tasks. A cluster manager allocates resources and launches executors. Executors run the actual tasks against data partitions and can store intermediate or cached data. When an action is triggered, Spark creates a job, divides it into stages based largely on dependency and shuffle boundaries, and then creates tasks for the partitions in each stage. The driver schedules these tasks on executors, which process the data in parallel.

---

# 49. Key Takeaways

1. A Spark application consists primarily of a **Driver and Executors**.
2. The **Driver coordinates** the application.
3. **Executors execute tasks** and process data partitions.
4. A **Cluster Manager allocates resources**.
5. `SparkSession` is the main entry point for modern Spark applications.
6. Transformations are **lazy**.
7. Actions trigger **jobs**.
8. Jobs are divided into **stages**.
9. Stages are divided into **tasks**.
10. Tasks process **partitions**.
11. Narrow transformations generally avoid major data redistribution.
12. Wide transformations generally involve **shuffle**.
13. Shuffle commonly creates stage boundaries.
14. Multiple tasks can execute in parallel depending on available resources.
15. Executor cores influence the number of tasks that can run concurrently.
16. Partition count influences parallelism.
17. Catalyst optimizes Spark SQL/DataFrame queries.
18. `explain()` helps inspect the physical execution plan.
19. `Exchange` in a physical plan commonly indicates data redistribution.
20. Spark UI helps monitor jobs, stages, tasks, executors, SQL, and storage.
21. Lineage enables Spark to recompute lost intermediate data.
22. Worker nodes are machines; executors are processes running on them.
23. More executors do not automatically guarantee better performance.
24. Understanding Spark architecture is the foundation for understanding Spark performance.

---


