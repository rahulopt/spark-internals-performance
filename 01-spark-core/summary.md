# 🧠 Spark Core — Summary

This section covers the **core concepts behind Apache Spark execution**.

The goal is not just to learn PySpark APIs, but to understand:

> **How Spark actually executes our code.**

---

# 📚 Topics Covered

```text
01. Spark Application
02. Reading Data
03. Actions
04. Lazy Evaluation & DAG
05. Partitions
06. Jobs, Stages, Tasks & Executors
07. Parallelism
08. Narrow Transformations
09. Shuffle & groupBy
10. Practical Shuffle
11. Join Operations — Part 1
12. Join Operations — Part 2
13. show() vs count()
14. Repartition
15. Repartition — Real World
16. Coalesce vs Repartition
```

---

# 1️⃣ Spark Application

A Spark application is a program written using Spark APIs and executed on a Spark cluster.

Main components:

```text
                    Spark Application
                           |
                           ↓
                        Driver
                           |
                +----------+----------+
                |                     |
                ↓                     ↓
         Cluster Manager          Spark UI
                |
                ↓
           Executors
                |
                ↓
              Tasks
```

### Driver

The Driver:

* Runs the main application
* Creates `SparkSession`
* Builds the execution plan
* Creates jobs
* Coordinates execution

### Executors

Executors:

* Run tasks
* Process partitions
* Store cached data
* Report results/status to the Driver

---

# 2️⃣ SparkSession

`SparkSession` is the main entry point for working with Spark DataFrames and SQL.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .getOrCreate()
```

Think:

```text
SparkSession
     ↓
Entry point to Spark
```

---

# 3️⃣ Transformations vs Actions

This is one of the most important concepts.

## Transformations

Transformations create a new DataFrame but do not immediately execute the computation.

Examples:

```python
df.select(...)
df.filter(...)
df.withColumn(...)
df.join(...)
df.groupBy(...)
df.repartition(...)
df.coalesce(...)
```

Conceptually:

```text
Transformation
      ↓
New Logical Plan
      ↓
No execution yet
```

---

## Actions

Actions trigger execution.

Examples:

```python
df.show()
df.count()
df.collect()
df.take()
df.write.parquet(...)
```

Conceptually:

```text
Action
   ↓
Job
   ↓
Execution
```

---

# 4️⃣ Lazy Evaluation

Spark uses **lazy evaluation**.

Example:

```python
result = (
    df
    .filter("age > 30")
    .select("name", "age")
)
```

Nothing is immediately executed.

Spark builds a plan:

```text
Read Data
   ↓
Filter
   ↓
Select
```

Then:

```python
result.show()
```

triggers execution.

---

# 5️⃣ DAG

DAG = **Directed Acyclic Graph**

Spark builds a DAG representing the sequence of transformations.

Example:

```python
result = (
    df
    .filter("age > 30")
    .select("name")
    .groupBy("name")
    .count()
)
```

Conceptually:

```text
Read
 ↓
Filter
 ↓
Select
 ↓
GroupBy
 ↓
Shuffle
 ↓
Aggregation
```

Spark uses this information to build an optimized execution plan.

---

# 6️⃣ Partitions

Spark does not normally process an entire dataset as one unit.

It divides data into partitions.

```text
Large Dataset
      ↓
+-----+-----+-----+-----+
| P0  | P1  | P2  | P3  |
+-----+-----+-----+-----+
```

Each partition can be processed independently.

This enables parallelism.

---

# 7️⃣ Partition → Task

A useful mental model:

```text
Partition
    ↓
Task
    ↓
Executor
```

For a stage:

```text
4 Partitions
     ↓
4 Tasks
```

Conceptually:

```text
P0 → Task 0
P1 → Task 1
P2 → Task 2
P3 → Task 3
```

Therefore:

> **Partition count strongly influences the number of tasks.**

---

# 8️⃣ Parallelism

Parallelism means Spark can process multiple partitions at the same time.

Suppose:

```text
8 partitions
+
4 available cores
```

Spark can approximately run:

```text
4 tasks
```

at once.

Then another batch:

```text
4 tasks
```

runs.

Conceptually:

```text
Wave 1 → T0 T1 T2 T3
Wave 2 → T4 T5 T6 T7
```

---

# 9️⃣ Narrow Transformation

A narrow transformation does not require records to move between partitions.

Examples:

```python
filter()
select()
withColumn()
```

Conceptually:

```text
P0 → P0
P1 → P1
P2 → P2
P3 → P3
```

No major redistribution is required.

---

# 🔟 Wide Transformation

A wide transformation requires data to be redistributed between partitions.

Examples:

```python
groupBy()
join()
distinct()
repartition()
```

Conceptually:

```text
P0 ─┐
P1 ─┤
P2 ─┼──→ Shuffle ──→ New Partitions
P3 ─┘
```

Wide transformations generally create a **shuffle boundary**.

---

# 1️⃣1️⃣ Shuffle

Shuffle means:

> **Redistributing data across partitions.**

Example:

```python
df.groupBy("customer_id").count()
```

Spark needs records with the same `customer_id` to be brought together.

```text
Input Partitions
      ↓
Partition by Key
      ↓
Shuffle
      ↓
New Partitions
      ↓
Aggregation
```

Shuffle can involve:

* Network I/O
* Disk I/O
* Memory
* CPU
* Serialization

Therefore:

> **Shuffle is often one of the expensive parts of Spark execution.**

---

# 1️⃣2️⃣ Jobs

A Spark **job** is triggered by an action.

Example:

```python
df.show()
```

creates a job.

Another action:

```python
df.count()
```

can create another job.

```text
Action
  ↓
Job
```

---

# 1️⃣3️⃣ Stages

A job is divided into stages.

Stage boundaries are commonly created around shuffle operations.

Example:

```text
Stage 1
Read
 ↓
Filter
 ↓
Shuffle Write
 ↓
----------------
Stage Boundary
----------------
 ↓
Stage 2
Shuffle Read
 ↓
Aggregation
 ↓
Result
```

---

# 1️⃣4️⃣ Tasks

A stage is divided into tasks.

Typically:

```text
1 Partition
     ↓
1 Task
```

Example:

```text
Stage
 ↓
100 partitions
 ↓
100 tasks
```

Tasks are executed by executors.

---

# 1️⃣5️⃣ Complete Execution Hierarchy

This is one of the most important mental models.

```text
Application
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
```

A more accurate operational view:

```text
                Driver
                  |
                Job
                  |
             +----+----+
             |         |
          Stage 1   Stage 2
             |         |
           Tasks     Tasks
             |         |
        Partitions  Partitions
             \         /
              Executors
```

---

# 1️⃣6️⃣ `show()` vs `count()`

Both are actions:

```python
df.show()
df.count()
```

But they have different requirements.

### `show()`

Used to inspect rows.

```python
df.show(10)
```

It requests a limited amount of output.

### `count()`

Needs the total number of rows.

```python
df.count()
```

This generally requires processing all relevant partitions.

Important:

> Don't think `show()` is always cheap and `count()` is always expensive. The transformations before the action determine much of the actual work.

---

# 1️⃣7️⃣ `collect()` vs `show()`

### `show()`

```python
df.show()
```

Displays rows.

### `collect()`

```python
df.collect()
```

Brings all rows to the Driver.

Potential problem:

```text
Large DataFrame
      ↓
collect()
      ↓
Driver Memory
      ↓
OOM
```

Therefore:

> Avoid `collect()` on large datasets.

---

# 1️⃣8️⃣ Repartition

`repartition()` changes the partitioning of a DataFrame.

```python
df.repartition(100)
```

It can:

* Increase partitions
* Decrease partitions
* Redistribute data
* Partition by a key

Example:

```python
df.repartition(
    100,
    "customer_id"
)
```

---

# 1️⃣9️⃣ Repartition Causes Shuffle

The most important thing:

```python
df.repartition(100)
```

causes a shuffle.

Conceptually:

```text
Old Partitions
      ↓
   Shuffle
      ↓
100 New Partitions
```

Therefore:

> **Don't use repartition blindly.**

---

# 2️⃣0️⃣ Repartition by Key

Example:

```python
df.repartition(
    "customer_id"
)
```

Spark distributes records based on the key.

Conceptually:

```text
customer_id
     ↓
Hash Partitioning
     ↓
Target Partition
```

This can be useful for operations involving:

```text
groupBy(customer_id)
join(customer_id)
```

But Spark may already perform the required shuffle.

So inspect the plan before manually repartitioning.

---

# 2️⃣1️⃣ Coalesce

`coalesce()` is mainly used to reduce partitions.

```python
df.coalesce(10)
```

If we have:

```text
100 partitions
```

and need:

```text
10 partitions
```

then:

```python
df.coalesce(10)
```

can be more efficient than:

```python
df.repartition(10)
```

when redistribution is not required.

---

# 2️⃣2️⃣ Coalesce vs Repartition

The simplest comparison:

```text
REPARTITION
     ↓
Redistribute
     ↓
Shuffle
```

```text
COALESCE
     ↓
Combine existing partitions
     ↓
Usually avoids full shuffle
```

| Feature                         | Repartition | Coalesce                |
| ------------------------------- | ----------- | ----------------------- |
| Increase partitions             | ✅           | ❌                       |
| Decrease partitions             | ✅           | ✅                       |
| Shuffle                         | ✅           | Usually no full shuffle |
| Redistribute data               | ✅           | Limited                 |
| Key-based partitioning          | ✅           | ❌                       |
| Good for reducing files         | Sometimes   | ✅                       |
| Good for increasing parallelism | ✅           | ❌                       |

---

# 2️⃣3️⃣ When to Use What?

### Need more partitions?

```python
df.repartition(100)
```

### Need fewer partitions?

If you don't need redistribution:

```python
df.coalesce(20)
```

### Need fewer partitions + redistribution?

```python
df.repartition(20)
```

### Need partitioning by a key?

```python
df.repartition(
    100,
    "customer_id"
)
```

---

# 2️⃣4️⃣ Partition Count Is Not a Magic Formula

A common misconception is:

```text
Data Size / 128 MB = Number of Partitions
```

This can be a useful rule of thumb in certain contexts, but it is **not Spark's universal partition formula**.

Actual partitioning depends on things such as:

* Input file sizes
* File splitting
* `spark.sql.files.maxPartitionBytes`
* Input format
* Existing partitioning
* Shuffle configuration
* `spark.sql.shuffle.partitions`
* AQE
* Data distribution

Therefore:

> **Don't memorize 128 MB as Spark's partition formula.**

---

# 2️⃣5️⃣ Input Partitions vs Shuffle Partitions

These are different concepts.

### Input

```text
S3 / HDFS / Files
       ↓
Input Partitions
```

### Shuffle

```text
groupBy / join / repartition
       ↓
Shuffle
       ↓
Shuffle Partitions
```

They do not have to be equal.

---

# 2️⃣6️⃣ `spark.sql.shuffle.partitions`

Spark has a configuration for shuffle partitions:

```python
spark.conf.get(
    "spark.sql.shuffle.partitions"
)
```

You can configure it:

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    500
)
```

This influences the number of shuffle partitions for relevant operations.

---

# 2️⃣7️⃣ AQE

AQE = **Adaptive Query Execution**

Modern Spark can use runtime statistics to optimize execution.

```text
Initial Plan
     ↓
Execute
     ↓
Runtime Statistics
     ↓
Adaptive Optimization
```

AQE can help with:

* Coalescing small shuffle partitions
* Skewed joins
* Dynamic execution decisions

Therefore:

> Manual partition tuning is not always necessary.

---

# 2️⃣8️⃣ Real-World Partition Tuning

Don't start with:

```python
df.repartition(1000)
```

Instead:

```text
1. Check current partitions
        ↓
2. Check physical plan
        ↓
3. Run the job
        ↓
4. Open Spark UI
        ↓
5. Identify bottleneck
        ↓
6. Tune partitions if necessary
        ↓
7. Measure again
```

Useful commands:

```python
df.rdd.getNumPartitions()
```

```python
df.explain("formatted")
```

```python
spark.conf.get(
    "spark.sql.shuffle.partitions"
)
```

---

# 2️⃣9️⃣ Spark UI Mental Model

When debugging performance, look at:

```text
Spark UI
   |
   +── Jobs
   |
   +── Stages
   |
   +── Tasks
   |
   +── Shuffle Read
   |
   +── Shuffle Write
   |
   +── Task Duration
```

Look for:

### Too few partitions

```text
Few tasks
+
Large task sizes
```

### Too many partitions

```text
Huge number of tiny tasks
```

### Data skew

```text
Most tasks → fast
One/few tasks → extremely slow
```

### Expensive shuffle

```text
Large Shuffle Read
Large Shuffle Write
```

---

# 🧠 The Complete Spark Mental Model

The whole Spark Core section can be understood through this flow:

```text
                 PySpark Code
                      |
                      ↓
                Transformations
                      |
                      ↓
                 Lazy Evaluation
                      |
                      ↓
                     DAG
                      |
                      ↓
               Physical Plan
                      |
                      ↓
                   Action
                      |
                      ↓
                    Job
                      |
                +-----+-----+
                |           |
             Stage       Stage
                |           |
              Tasks        Tasks
                |           |
           Partitions   Partitions
                |           |
                +-----+-----+
                      |
                      ↓
                  Executors
```

---

# 🔥 Narrow vs Wide

Another important mental model:

```text
NARROW
------

P0 → P0
P1 → P1
P2 → P2
P3 → P3

No major redistribution


WIDE
----

P0 ─┐
P1 ─┤
P2 ─┼──→ Shuffle → New Partitions
P3 ─┘
```

### Narrow Examples

```python
filter()
select()
withColumn()
```

### Wide Examples

```python
groupBy()
join()
distinct()
repartition()
```

---

# 🔥 Partitioning Mental Model

```text
                 Dataset
                    |
                    ↓
               Partitions
                    |
          +---------+---------+
          |                   |
          ↓                   ↓
       Narrow              Wide
          |                   |
          ↓                   ↓
  Same partition         Shuffle
       lineage               |
                              ↓
                     New partitions
```

---

# 🔥 Repartition Mental Model

```text
Current Partitions
       |
       ↓
Need redistribution?
       |
   +---+---+
   |       |
  Yes      No
   |       |
   ↓       ↓
repartition   coalesce
   |
   ↓
Shuffle
   |
   ↓
New partition layout
```

---

# ⚠️ Common Mistakes

### ❌ "Every transformation creates a job"

Wrong.

```text
Transformation
    ↓
Lazy
```

Action triggers the job.

---

### ❌ "Every transformation creates a stage"

Wrong.

Stages are mainly separated by shuffle boundaries.

---

### ❌ "Every action creates a new stage"

Not exactly.

An action creates a **job**, and that job may contain multiple stages.

---

### ❌ "More partitions always means faster"

Wrong.

Too many partitions can create scheduling overhead.

---

### ❌ "128 MB is Spark's partition formula"

Wrong.

It's only a rough rule of thumb in some contexts.

---

### ❌ "Repartition is free"

Wrong.

```text
repartition()
     ↓
Shuffle
     ↓
Cost
```

---

### ❌ "Coalesce is always better"

Wrong.

If you need redistribution, `repartition()` may be the correct choice.

---

### ❌ "Same number of partitions means same performance"

Wrong.

Partition **distribution** also matters.

---

### ❌ "collect() is safe for inspecting data"

Wrong.

For large data:

```python
df.collect()
```

can overload Driver memory.

Prefer:

```python
df.show()
```

---

# 🧪 Useful Commands Cheat Sheet

### Check partition count

```python
df.rdd.getNumPartitions()
```

### Display data

```python
df.show()
```

### Count rows

```python
df.count()
```

### Inspect physical plan

```python
df.explain("formatted")
```

### Repartition

```python
df.repartition(100)
```

### Repartition by key

```python
df.repartition(
    100,
    "customer_id"
)
```

### Coalesce

```python
df.coalesce(20)
```

### Check shuffle partition configuration

```python
spark.conf.get(
    "spark.sql.shuffle.partitions"
)
```

### Change shuffle partitions

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    500
)
```

---

# 📊 Quick Comparison Table

| Concept           | Main Idea                                    |
| ----------------- | -------------------------------------------- |
| Spark Application | Complete Spark program                       |
| Driver            | Coordinates application                      |
| Executor          | Runs tasks                                   |
| Transformation    | Lazy operation                               |
| Action            | Triggers execution                           |
| DAG               | Execution dependency graph                   |
| Job               | Created by an action                         |
| Stage             | Set of tasks separated by shuffle boundaries |
| Task              | Unit of execution                            |
| Partition         | Unit of data processing                      |
| Narrow            | No major data redistribution                 |
| Wide              | Requires shuffle                             |
| Shuffle           | Redistributes data                           |
| Repartition       | Redistribute partitions                      |
| Coalesce          | Reduce partitions efficiently                |
| AQE               | Runtime adaptive optimization                |

---

# 🎯 What You Should Be Able to Explain

After completing Spark Core, you should be able to answer:

### 1. What happens when I call `filter()`?

```text
Lazy transformation
```

### 2. What happens when I call `show()`?

```text
Action
 ↓
Job
 ↓
Execution
```

### 3. What creates a stage boundary?

```text
Usually a shuffle dependency
```

### 4. What is a task?

```text
Work performed on a partition
```

### 5. Why does Spark use partitions?

```text
Parallel processing
```

### 6. Why is shuffle expensive?

```text
Network + Disk + Memory + CPU
```

### 7. When should I use repartition?

```text
When I need redistribution
```

### 8. When should I use coalesce?

```text
When I mainly need fewer partitions
```

### 9. Why can one task be much slower?

```text
Possible data skew / uneven partition size
```

### 10. How do I debug Spark performance?

```text
explain()
+
Spark UI
+
Partition metrics
+
Shuffle metrics
```

---

# ⭐ Final Spark Core Mental Model

If you remember only one diagram from this section, remember this:

```text
                        SPARK
                          |
                          ↓
                  Transformations
                          |
                          ↓
                  Lazy Evaluation
                          |
                          ↓
                         DAG
                          |
                          ↓
                    Physical Plan
                          |
                          ↓
                       ACTION
                          |
                          ↓
                         JOB
                          |
                    +-----+-----+
                    |           |
                  STAGE       STAGE
                    |           |
                  TASKS       TASKS
                    |           |
              PARTITIONS   PARTITIONS
                    |           |
                    +-----+-----+
                          |
                          ↓
                      EXECUTORS
                          |
                          ↓
                       RESULT
```

And whenever you see:

```text
groupBy()
join()
distinct()
repartition()
```

think:

```text
          WIDE OPERATION
                ↓
             SHUFFLE
                ↓
        Data redistribution
                ↓
          Stage boundary
```

---

# 🚀 Final Takeaway

> **Spark performance starts with understanding partitions and data movement.**

The most important questions to ask are:

```text
How much data?
      ↓
How many partitions?
      ↓
How large is each partition?
      ↓
Is the data balanced?
      ↓
Is shuffle happening?
      ↓
How many tasks are running?
      ↓
Are executors being utilized?
```

Once you understand this flow, Spark's:

* Jobs
* Stages
* Tasks
* Executors
* Parallelism
* Shuffle
* Repartition
* Coalesce
* AQE

all start fitting together as one system.

> **Don't optimize Spark by memorizing numbers. Understand the data movement, measure it in Spark UI, and then tune the bottleneck.**
