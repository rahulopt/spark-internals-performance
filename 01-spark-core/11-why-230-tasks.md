# Why Did Spark Create 230 Tasks?

One of the most common questions beginners ask after opening the Spark UI is:

> **"Why is Spark running exactly 230 tasks? Where did this number come from?"**

The answer is simple: **Spark creates one task for each partition in a stage.**

If your DataFrame has **230 partitions**, Spark will create **230 tasks** to process those partitions in parallel. A task is the smallest unit of work in Spark, and each task is responsible for processing exactly one partition.

```text
1 Partition = 1 Task
```

When Spark reads data from storage (such as S3 or HDFS), it splits the input into multiple partitions based on factors like file size, block size, and Spark's partitioning strategy. If the input is divided into **230 partitions**, the first stage will naturally contain **230 tasks**.

For example:

```python
df = spark.read.csv("s3://bucket/sales.csv")

print(df.rdd.getNumPartitions())
```

Output:

```text
230
```

When an action is executed:

```python
df.count()
```

Spark launches:

```text
Task 1   → Partition 1
Task 2   → Partition 2
Task 3   → Partition 3
...
Task 230 → Partition 230
```

which is exactly what you see in the Spark UI.

---

## Why Doesn't Every Stage Have 230 Tasks?

Spark creates tasks **stage by stage**, and each stage can have a different number of partitions.

For example:

```text
Stage 1
230 Partitions
↓
230 Tasks

Stage 2 (After Shuffle)
200 Partitions
↓
200 Tasks
```

This commonly happens after operations like:

- `groupBy()`
- `join()`
- `distinct()`
- `orderBy()`

These operations trigger a **shuffle**, where Spark redistributes data and creates a new set of partitions. By default, shuffle stages use:

```python
spark.conf.get("spark.sql.shuffle.partitions")
```

Output:

```text
200
```

So it's completely normal to see:

- Stage 1 → **230 Tasks**
- Stage 2 → **200 Tasks**
- Stage 3 → **50 Tasks**

Each stage depends on its own partition count.

---

## How to Check the Number of Partitions

```python
df.rdd.getNumPartitions()
```

---

## How to Change the Number of Tasks

Increase partitions:

```python
df = df.repartition(300)
```

Decrease partitions:

```python
df = df.coalesce(20)
```

Change the default shuffle partition count:

```python
spark.conf.set("spark.sql.shuffle.partitions", 300)
```

---

## Summary

- A **task** is the smallest unit of work in Spark.
- **One partition creates one task** in a stage.
- Reading data creates partitions based on file splits.
- Shuffle operations create new partitions (default: **200**).
- Different stages can have different numbers of tasks.
- If the Spark UI shows **230 tasks**, it simply means that stage contains **230 partitions**.

> **Remember:**  
> **1 Partition → 1 Task → Executed by an Executor Core**