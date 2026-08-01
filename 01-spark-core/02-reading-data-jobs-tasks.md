# Reading Data, Jobs, Stages, and Tasks

## Introduction

Reading data is usually the first step in every Spark application. Whether the data comes from a CSV file, Parquet file, JSON file, a database, or cloud storage like Amazon S3, Spark follows the same execution model.

At first glance, reading a file may seem like a single operation, but internally Spark performs several steps before the data is available as a DataFrame. Understanding these steps helps explain how Spark creates **Jobs**, **Stages**, and **Tasks**, and why Spark can process massive datasets efficiently.

---

# Reading Data

Spark can read data from many different sources.

Some common examples include:

- CSV
- JSON
- Parquet
- ORC
- Avro
- Hive Tables
- Relational Databases (JDBC)
- Amazon S3
- Azure Data Lake
- Google Cloud Storage

Example:

```python
orders_df = spark.read \
    .option("header", "true") \
    .csv("orders.csv")
```

This creates a DataFrame, but **Spark has not read the entire file yet.**

Why?

Because Spark uses **Lazy Evaluation**.

---

# Lazy Evaluation

Spark delays execution until an **Action** is called.

Transformations such as

- select()
- filter()
- withColumn()
- groupBy()

only describe **what** needs to be done.

Spark simply records these operations and builds a logical execution plan.

No computation happens until an action is triggered.

Example:

```python
df = spark.read.csv("orders.csv")

df = df.filter("quantity > 2")

df = df.select("customer_id", "quantity")
```

Nothing is executed yet.

When we call:

```python
df.show()
```

Spark starts executing the entire pipeline.

---

# What Happens When an Action is Called?

Suppose we execute:

```python
orders_df.show()
```

Internally, Spark performs the following steps:

1. Driver receives the action.
2. Driver analyzes the DAG.
3. Spark optimizes the execution plan.
4. Driver creates one or more Jobs.
5. Each Job is divided into Stages.
6. Each Stage is divided into Tasks.
7. Tasks are sent to Executors.
8. Executors process the data.
9. Results are returned to the Driver.

```
show()

     │

Driver

     │

Optimize DAG

     │

Job

     │

Stages

     │

Tasks

     │

Executors

     │

Result
```

---

# Job

A **Job** is created whenever an **Action** is executed.

Examples of Actions:

```python
show()

count()

collect()

write()

take()

first()
```

Each action generally creates a new Job.

Example:

```python
orders.show()

orders.count()

orders.collect()
```

This creates:

```
Job 0

Job 1

Job 2
```

---

# Stage

A Job is divided into one or more **Stages**.

Spark creates a new Stage whenever it encounters a **Shuffle Boundary**.

Operations that usually trigger a shuffle include:

- groupBy()
- join()
- distinct()
- repartition()
- orderBy()

Example:

```python
orders.groupBy("region").count().show()
```

Possible execution:

```
Job

│

├── Stage 0
│
└── Stage 1
```

Stage 0 reads the data.

Stage 1 performs the aggregation after the shuffle.

---

# Task

A **Task** is the smallest unit of execution in Spark.

One Task processes **one data partition**.

If a DataFrame has:

```
8 Partitions
```

Spark creates:

```
8 Tasks
```

These tasks can run in parallel across multiple executors.

Example:

```
Partitions

P1

P2

P3

P4

P5

P6

P7

P8

↓

Tasks

T1

T2

T3

T4

T5

T6

T7

T8
```

---

# Relationship Between Partitions and Tasks

Spark creates exactly **one task per partition** for each stage.

Example:

```
16 Partitions

↓

16 Tasks
```

If there are four executors:

```
Executor 1

Task 1

Task 2

Task 3

Task 4

Executor 2

Task 5

Task 6

Task 7

Task 8

Executor 3

Task 9

Task 10

Task 11

Task 12

Executor 4

Task 13

Task 14

Task 15

Task 16
```

All tasks execute in parallel, depending on the available CPU cores and task slots.

---

# Example Execution

```python
orders = spark.read.csv("orders.csv", header=True)

orders = orders.filter("quantity > 2")

orders = orders.groupBy("region").count()

orders.show()
```

Execution flow:

```
Read File

↓

Filter

↓

Group By

↓

Show()

↓

Job Created

↓

Stage 0

(Read + Filter)

↓

Shuffle

↓

Stage 1

(Group By)

↓

Tasks

↓

Executors

↓

Results
```

---

# Viewing Jobs in Spark UI

Open Spark UI:

```
http://localhost:4040
```

Navigate to the **Jobs** tab.

There you can see:

- Job ID
- Duration
- Number of Stages
- Status
- DAG Visualization

Click a Job to see:

- Stages
- Tasks
- Shuffle Read
- Shuffle Write
- Input Size
- Output Size

---

# Key Points

- Reading a file does not immediately execute the computation.
- Spark uses **Lazy Evaluation** and waits for an Action.
- Every Action creates a Job.
- Every Job consists of one or more Stages.
- Stages are divided into Tasks.
- One Task processes one data partition.
- Tasks run in parallel on Executors.
- Spark UI helps visualize Jobs, Stages, Tasks, and execution metrics.