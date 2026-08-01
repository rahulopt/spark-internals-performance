# Jobs, Stages, Tasks, and Executors

## Introduction

Whenever a Spark application executes an **Action**, Spark follows a well-defined execution process. The Driver analyzes the execution plan, creates one or more **Jobs**, divides each Job into **Stages**, splits each Stage into **Tasks**, and finally sends those Tasks to **Executors** for parallel execution.

Understanding the relationship between Jobs, Stages, Tasks, and Executors is essential for understanding how Spark processes data efficiently.

---

# Spark Execution Flow

```
Spark Application
        │
        ▼
    Driver Program
        │
        ▼
       Action
        │
        ▼
        Job
        │
        ▼
      Stages
        │
        ▼
       Tasks
        │
        ▼
     Executors
        │
        ▼
      Results
```

---

# What is a Job?

A **Job** is the highest level of execution in Spark.

Whenever an **Action** is called, Spark creates a new Job.

For example:

```python
orders.show()

orders.count()

orders.write.parquet("output/")
```

Each Action creates a separate Job.

```
show()

↓

Job 0


count()

↓

Job 1


write()

↓

Job 2
```

A Spark application can contain multiple Jobs.

---

# What is a Stage?

A **Stage** is a collection of Tasks that can be executed together without requiring data to be shuffled across the cluster.

Spark divides a Job into one or more Stages based on **shuffle boundaries**.

Operations that typically create a new Stage include:

- groupBy()
- join()
- distinct()
- repartition()
- orderBy()
- dropDuplicates()

These operations require data to move between partitions, which is known as a **Shuffle**.

Example:

```python
orders.groupBy("region").count().show()
```

Execution:

```
Job

│

├── Stage 0
│      Read CSV
│      Filter
│
└── Stage 1
       Shuffle
       Group By
```

---

# Narrow Stage vs Shuffle Stage

### Narrow Stage

Operations that do not require data movement remain in the same Stage.

Examples:

- filter()
- select()
- withColumn()
- map()

```
Read CSV

↓

Filter

↓

Select

↓

Same Stage
```

---

### Shuffle Stage

Operations requiring data movement create a new Stage.

Examples:

- join()
- groupBy()
- distinct()
- repartition()
- orderBy()

```
Stage 0

↓

Shuffle

↓

Stage 1
```

---

# What is a Task?

A **Task** is the smallest unit of execution in Spark.

Each Task processes **exactly one partition** within a Stage.

Example:

```
8 Partitions

↓

8 Tasks
```

If a Stage has sixteen partitions, Spark creates sixteen Tasks.

Tasks belonging to the same Stage execute independently and can run in parallel.

---

# Task Execution

Suppose a DataFrame has six partitions.

```
Partition 1

Partition 2

Partition 3

Partition 4

Partition 5

Partition 6
```

Spark creates:

```
Task 1

Task 2

Task 3

Task 4

Task 5

Task 6
```

Each Task is assigned to an available Executor.

---

# What is an Executor?

An **Executor** is a JVM process running on a Worker Node.

Executors are launched by the Cluster Manager after the Driver requests resources.

Each Executor contains:

- Memory
- CPU Cores
- Task Slots

Responsibilities of an Executor:

- Execute Tasks
- Cache Data
- Perform Shuffle Operations
- Write Output
- Return Results to the Driver

Example:

```
Driver

↓

Executor 1

Task 1

Task 2


Executor 2

Task 3

Task 4


Executor 3

Task 5

Task 6
```

Executors remain alive for the lifetime of the Spark application unless dynamic allocation is enabled.

---

# Complete Execution Example

```python
orders = spark.read.csv("orders.csv", header=True)

result = (
    orders
    .filter("quantity > 2")
    .groupBy("region")
    .count()
)

result.show()
```

Spark performs the following steps:

### Step 1

Read the CSV file.

### Step 2

Apply the filter transformation.

### Step 3

Encounter `groupBy()`.

Since `groupBy()` requires a shuffle, Spark creates another Stage.

### Step 4

`show()` triggers execution.

Spark creates a Job.

### Step 5

Each Stage is divided into Tasks.

### Step 6

Tasks are assigned to Executors.

### Step 7

Executors execute the Tasks in parallel.

### Step 8

Results are returned to the Driver.

Execution Flow:

```
Read CSV
      │
      ▼
Filter
      │
      ▼
Group By
      │
      ▼
show()
      │
      ▼
Job
      │
      ▼
Stage 0
(Read + Filter)
      │
      ▼
Shuffle
      │
      ▼
Stage 1
(Group By)
      │
      ▼
Tasks
      │
      ▼
Executors
      │
      ▼
Driver
      │
      ▼
Display Results
```

---

# Relationship Between Jobs, Stages, Tasks, and Executors

```
Spark Application
        │
        ▼
      Job
        │
        ▼
 ┌───────────────┐
 │               │
 ▼               ▼
Stage 0      Stage 1
 │               │
 ▼               ▼
Tasks         Tasks
 │               │
 └──────┬────────┘
        ▼
    Executors
        │
        ▼
      Driver
```

---

# Viewing Execution in Spark UI

Open Spark UI:

```
http://localhost:4040
```

Useful tabs:

### Jobs

- Job status
- Duration
- DAG visualization

### Stages

- Stage details
- Shuffle Read
- Shuffle Write
- Number of Tasks

### Tasks

- Task duration
- Input and output metrics
- Failed Tasks
- GC Time

### Executors

- Active Executors
- Memory Usage
- CPU Time
- Task Counts

Spark UI is the best place to understand how Spark executed your application.

---

# Best Practices

- Keep transformations narrow whenever possible.
- Minimize unnecessary shuffle operations.
- Choose an appropriate number of partitions.
- Monitor Jobs, Stages, Tasks, and Executors using the Spark UI.
- Investigate slow stages to identify skew or shuffle bottlenecks.

---

# Key Takeaways

- Every **Action** creates a **Job**.
- Every **Job** consists of one or more **Stages**.
- Every **Stage** consists of multiple **Tasks**.
- Every **Task** processes exactly one partition.
- **Executors** execute Tasks in parallel on Worker Nodes.
- Shuffle operations create new Stages.
- Understanding Jobs, Stages, Tasks, and Executors is fundamental to understanding Spark's execution model and optimizing Spark applications.