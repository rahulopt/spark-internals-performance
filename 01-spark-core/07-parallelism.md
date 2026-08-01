# Parallelism in Apache Spark

## Introduction

One of the main reasons Apache Spark is able to process massive datasets efficiently is **parallelism**.

Instead of processing data sequentially on a single machine, Spark divides the data into multiple partitions and executes tasks simultaneously across multiple CPU cores and executors.

The more tasks Spark can execute in parallel, the faster the application can complete, provided sufficient cluster resources are available.

---

# What is Parallelism?

**Parallelism** is the ability to execute multiple tasks at the same time.

In Spark, parallelism is achieved by:

- Dividing data into partitions
- Creating one task for each partition
- Executing those tasks simultaneously on different executors

```
Dataset

      │

      ▼

Partitions

P1   P2   P3   P4

      │

      ▼

Tasks

T1   T2   T3   T4

      │

      ▼

Executors

E1   E2   E3   E4
```

---

# Why is Parallelism Important?

Without parallelism, Spark would process data one record at a time on a single machine.

With parallelism, Spark distributes the workload across multiple machines and CPU cores.

Benefits include:

- Faster execution
- Better CPU utilization
- Efficient use of cluster resources
- Ability to process very large datasets
- Improved scalability

---

# How Spark Achieves Parallelism

Spark follows these steps:

1. Read the dataset.
2. Divide the dataset into partitions.
3. Create one task for each partition.
4. Assign tasks to available executors.
5. Execute tasks in parallel.
6. Combine the results.

Example:

```python
orders = spark.read.csv("orders.csv", header=True)

orders.filter("quantity > 2").show()
```

Execution Flow:

```
Read CSV

↓

8 Partitions

↓

8 Tasks

↓

Executors

↓

Results
```

---

# Relationship Between Partitions and Parallelism

The amount of parallelism is directly related to the number of partitions.

```
1 Partition

↓

1 Task

↓

Low Parallelism
```

```
8 Partitions

↓

8 Tasks

↓

Higher Parallelism
```

However, creating more partitions does not always improve performance.

---

# Role of CPU Cores

Tasks execute using CPU cores.

Suppose a cluster has:

- 2 Executors
- 4 CPU Cores per Executor

Total available cores:

```
2 × 4 = 8 Cores
```

Spark can execute up to **8 Tasks simultaneously**.

Example:

```
Executor 1

Core 1 → Task 1

Core 2 → Task 2

Core 3 → Task 3

Core 4 → Task 4


Executor 2

Core 1 → Task 5

Core 2 → Task 6

Core 3 → Task 7

Core 4 → Task 8
```

If there are more tasks than available cores, the remaining tasks wait until a core becomes free.

---

# Number of Partitions vs Number of Cores

Example 1:

```
4 Partitions

8 Cores
```

Only four cores are used.

Four cores remain idle.

---

Example 2:

```
16 Partitions

8 Cores
```

Eight tasks run immediately.

The remaining eight tasks execute after the first batch completes.

---

Choosing an appropriate number of partitions helps maximize resource utilization.

---

# Default Parallelism

Spark provides a configuration that determines the default level of parallelism.

```python
spark.sparkContext.defaultParallelism
```

The value generally depends on:

- Number of executor cores
- Cluster manager
- Deployment mode

---

# Controlling Parallelism

## repartition()

Increase or redistribute partitions.

```python
orders = orders.repartition(12)
```

Creates a full shuffle and redistributes the data evenly.

---

## coalesce()

Reduce the number of partitions.

```python
orders = orders.coalesce(4)
```

Attempts to reduce partitions while avoiding a full shuffle.

---

# Parallelism During Shuffle

Operations such as:

- groupBy()
- join()
- distinct()
- orderBy()

require a shuffle.

The number of shuffle partitions is controlled by:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Default value:

```text
200
```

You can change it:

```python
spark.conf.set("spark.sql.shuffle.partitions", 100)
```

---

# Too Little Parallelism

Problems:

- Low CPU utilization
- Longer execution time
- Executors remain idle

Example:

```
100 GB Dataset

↓

1 Partition

↓

1 Task

↓

Very Slow
```

---

# Too Much Parallelism

Problems:

- Too many tasks
- Increased scheduling overhead
- More context switching
- Lower overall performance

Example:

```
100 MB Dataset

↓

10,000 Partitions

↓

10,000 Tasks
```

Spark spends more time scheduling tasks than processing data.

---

# Best Practices

- Create enough partitions to utilize all available CPU cores.
- Avoid creating too few or too many partitions.
- Use `repartition()` to increase or evenly distribute partitions.
- Use `coalesce()` to reduce partitions efficiently.
- Tune `spark.sql.shuffle.partitions` based on your workload.
- Use the Spark UI to monitor task execution and executor utilization.

---

# Parallelism vs Concurrency

| Parallelism | Concurrency |
|-------------|-------------|
| Multiple tasks execute at the same time. | Multiple tasks make progress over time but may not execute simultaneously. |
| Uses multiple CPU cores or executors. | Can occur on a single CPU core through task switching. |
| Focuses on improving execution speed. | Focuses on managing multiple tasks efficiently. |

Spark primarily achieves high performance through **parallelism**.

---

# Key Takeaways

- Parallelism is the ability to execute multiple tasks simultaneously.
- Spark achieves parallelism by dividing data into partitions.
- One partition creates one task.
- Tasks are executed by executors using available CPU cores.
- The number of partitions influences the level of parallelism.
- Too few partitions underutilize resources, while too many increase scheduling overhead.
- Proper partitioning and resource allocation are essential for achieving optimal Spark performance.