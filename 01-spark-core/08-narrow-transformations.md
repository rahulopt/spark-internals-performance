# Narrow Transformations in Apache Spark

## Introduction

A **Transformation** is an operation that creates a new DataFrame or RDD from an existing one. Transformations are **lazy**, meaning Spark does not execute them immediately. Instead, it records the transformations and waits until an **Action** is called.

Based on how data flows between partitions, Spark classifies transformations into two categories:

- **Narrow Transformations**
- **Wide Transformations**

This chapter focuses on **Narrow Transformations**.

---

# What is a Narrow Transformation?

A **Narrow Transformation** is a transformation where each output partition depends on **only one input partition**.

Since data does not need to move between partitions, **no shuffle** occurs.

```
Partition 1  ─────► Partition 1

Partition 2  ─────► Partition 2

Partition 3  ─────► Partition 3
```

Each partition is processed independently.

---

# Characteristics of Narrow Transformations

- No shuffle required.
- Faster execution.
- Lower network communication.
- Lower disk I/O.
- Can be pipelined into a single stage.
- Better performance than wide transformations.

---

# Why are Narrow Transformations Faster?

Since each output partition depends on only one input partition:

- Spark doesn't redistribute data across the cluster.
- Executors process their own partitions locally.
- Network traffic is minimized.
- Spark can pipeline multiple narrow transformations together into a single stage.

Example:

```python
orders = (
    spark.read.csv("orders.csv", header=True)
         .filter("quantity > 2")
         .select("customer_id", "quantity")
         .withColumnRenamed("quantity", "qty")
)
```

Spark does **not** create a new stage for each transformation.

Instead, all these operations can be executed in the same stage.

---

# Common Narrow Transformations

## filter()

Returns only the rows that satisfy a condition.

```python
orders.filter("quantity > 2")
```

No shuffle occurs because each partition filters its own records.

---

## select()

Selects specific columns.

```python
orders.select("customer_id", "quantity")
```

No data movement is required.

---

## withColumn()

Adds or replaces a column.

```python
from pyspark.sql import functions as F

orders.withColumn(
    "revenue",
    F.col("quantity") * F.col("unit_price")
)
```

Each partition computes the new column independently.

---

## withColumnRenamed()

Renames an existing column.

```python
orders.withColumnRenamed("quantity", "qty")
```

Only metadata changes.

---

## drop()

Removes one or more columns.

```python
orders.drop("discount_pct")
```

No shuffle is required.

---

## alias()

Assigns a temporary name to a DataFrame or column.

```python
orders.select(
    F.col("quantity").alias("qty")
)
```

---

## limit()

Returns the first *N* rows.

```python
orders.limit(10)
```

> Although `limit()` is generally efficient, Spark may perform additional optimization depending on the execution plan.

---

## sample()

Returns a random sample of the dataset.

```python
orders.sample(0.20)
```

Each partition samples its own records independently.

---

## map() *(RDD API)*

Applies a function to every record.

```python
rdd.map(lambda x: x * 2)
```

No shuffle occurs.

---

## flatMap() *(RDD API)*

Returns zero or more output records for each input record.

```python
rdd.flatMap(lambda x: x.split())
```

---

# Execution Example

```python
orders = (
    spark.read.csv("orders.csv", header=True)
         .filter("quantity > 2")
         .select("customer_id", "quantity")
)

orders.show()
```

Execution Flow:

```
Read CSV
     │
     ▼
Filter
     │
     ▼
Select
     │
     ▼
show()
```

Internally:

```
Stage 0

Read CSV
      │
Filter
      │
Select
      │
show()
```

No shuffle occurs, so Spark executes all transformations within the same stage.

---

# Narrow Transformation Pipeline

Spark combines multiple narrow transformations into a pipeline.

```
Read CSV

      │

Filter

      │

Select

      │

withColumn

      │

drop

      │

Action
```

Instead of executing each operation separately, Spark executes them together after optimization.

---

# Narrow vs Wide Transformations

| Narrow Transformation | Wide Transformation |
|-----------------------|---------------------|
| No shuffle | Shuffle required |
| Faster execution | Slower execution |
| Local processing | Data moves across executors |
| Usually stays in the same stage | Creates a new stage |
| Lower network overhead | Higher network overhead |

---

# Best Practices

- Prefer narrow transformations whenever possible.
- Combine multiple narrow transformations before an action.
- Avoid unnecessary wide transformations.
- Use Spark UI to identify shuffle operations.
- Keep data local to reduce network overhead.

---

# Key Takeaways

- A **Narrow Transformation** processes each partition independently.
- No data is exchanged between partitions.
- Narrow transformations do **not** trigger a shuffle.
- Multiple narrow transformations are typically executed within the same stage.
- Narrow transformations are generally faster and more efficient than wide transformations because they minimize network communication and disk I/O.