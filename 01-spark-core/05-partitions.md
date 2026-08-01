# Partitions in Apache Spark

## Introduction

A **Partition** is the smallest logical unit into which data is divided in Apache Spark. Instead of processing an entire dataset on a single machine, Spark splits the data into multiple partitions and processes them in parallel across different executors.

Partitions are the foundation of Spark's distributed processing model. The number of partitions directly affects the level of parallelism and overall performance of a Spark application.

---

# What is a Partition?

A **Partition** is a chunk of data stored across the cluster.

When Spark reads a dataset, it automatically divides the data into multiple partitions.

For example:

```
orders.csv

──────────────────────────────

↓

Partition 1

Partition 2

Partition 3

Partition 4
```

Each partition can be processed independently by a separate task.

---

# Why Does Spark Use Partitions?

Instead of processing the entire dataset sequentially, Spark processes multiple partitions simultaneously.

Benefits include:

- Parallel processing
- Better resource utilization
- Faster execution
- Scalability across multiple machines
- Fault tolerance

Without partitions, Spark would not be able to process large datasets efficiently.

---

# One Partition = One Task

One of the most important concepts in Spark is:

> **One partition is processed by exactly one task within a stage.**

For example:

```
8 Partitions

↓

8 Tasks
```

If there are enough executor cores available, these tasks can execute in parallel.

---

# How Spark Creates Partitions

The number of partitions depends on several factors:

- File size
- Block size of the storage system
- Cluster configuration
- Spark configuration
- Operations such as `repartition()` or `coalesce()`

For example:

```python
orders = spark.read.csv("orders.csv", header=True)
```

Spark automatically creates partitions based on the input data.

---

# Viewing the Number of Partitions

You can check the number of partitions in a DataFrame using its underlying RDD.

```python
orders.rdd.getNumPartitions()
```

Output:

```
8
```

---

# Parallel Processing

Suppose a DataFrame has eight partitions.

```
Partition 1

Partition 2

Partition 3

Partition 4

Partition 5

Partition 6

Partition 7

Partition 8
```

Spark creates eight tasks.

If the cluster has four executor cores:

```
Core 1 → Task 1

Core 2 → Task 2

Core 3 → Task 3

Core 4 → Task 4

↓

Next four tasks execute after the first four finish.
```

More available cores allow more tasks to run simultaneously.

---

# Repartition()

`repartition()` increases or decreases the number of partitions by performing a full shuffle.

Syntax:

```python
df.repartition(numPartitions)
```

Example:

```python
orders = orders.repartition(10)
```

Now the DataFrame contains ten partitions.

Because it performs a shuffle, `repartition()` can be expensive.

---

# Repartition by Column

Spark can also repartition data based on one or more columns.

```python
orders = orders.repartition("region")
```

or

```python
orders = orders.repartition(8, "region")
```

Rows with the same value of `region` are likely to end up in the same partition, which can improve the performance of joins and aggregations.

---

# Coalesce()

`coalesce()` is used to reduce the number of partitions.

Unlike `repartition()`, it tries to avoid a full shuffle whenever possible.

Example:

```python
orders = orders.coalesce(4)
```

This reduces the number of partitions from the current value to four.

`coalesce()` is commonly used before writing data to reduce the number of output files.

---

# repartition() vs coalesce()

| repartition() | coalesce() |
|---------------|------------|
| Can increase or decrease partitions | Primarily decreases partitions |
| Performs a full shuffle | Minimizes or avoids shuffle |
| More expensive | More efficient when reducing partitions |
| Better for evenly distributing data | Better for reducing small files |

---

# Default Shuffle Partitions

Many wide transformations create shuffle partitions.

The default value is:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Default:

```text
200
```

You can change it:

```python
spark.conf.set("spark.sql.shuffle.partitions", 100)
```

Choosing an appropriate value can improve performance.

---

# Partitioning and Performance

Too Few Partitions:

- Poor CPU utilization
- Limited parallelism
- Slower execution

Example:

```
1 Partition

↓

1 Task

↓

Only one executor works
```

---

Too Many Partitions:

- Large scheduling overhead
- Many tiny tasks
- Reduced efficiency

Example:

```
10,000 Partitions

↓

10,000 Tasks

↓

High scheduling overhead
```

The goal is to choose a reasonable number of partitions based on the cluster resources and dataset size.

---

# Narrow vs Wide Transformations

Some transformations preserve partitions, while others require data movement.

### Narrow Transformation

No shuffle required.

Examples:

- filter()
- select()
- withColumn()
- map()

```
Partition 1

↓

Partition 1
```

---

### Wide Transformation

Requires shuffle.

Examples:

- groupBy()
- join()
- distinct()
- orderBy()
- repartition()

```
Partition 1

↓

Shuffle

↓

New Partitions
```

Wide transformations usually create new stages because of the shuffle.

---

# Best Practices

- Allow Spark to determine partitions when possible.
- Use `repartition()` when increasing partitions or redistributing data evenly.
- Use `coalesce()` when reducing partitions before writing data.
- Avoid creating too many small partitions.
- Monitor partition sizes using the Spark UI.
- Tune `spark.sql.shuffle.partitions` based on your workload.

---

# Key Takeaways

- A **Partition** is the smallest logical unit of data in Spark.
- Spark processes one partition with one task.
- More partitions generally allow greater parallelism.
- `repartition()` performs a full shuffle and can increase or decrease partitions.
- `coalesce()` is optimized for reducing partitions with minimal shuffle.
- Proper partitioning is essential for achieving good Spark performance.