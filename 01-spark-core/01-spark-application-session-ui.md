# Spark Application & Spark UI

## What is a Spark Application?

A **Spark Application** is a complete program that runs on a Spark cluster. It consists of:

- **Driver Program**
- **Cluster Manager** (Standalone, YARN, Kubernetes, Mesos)
- **Executors**
- **Tasks**

### Spark Application Flow

```
User Code
    │
    ▼
Driver Program
    │
    ├── Creates SparkSession
    ├── Builds DAG
    ├── Optimizes Execution Plan
    ├── Schedules Jobs
    ▼
Cluster Manager
    │
    ▼
Executors
    │
    ▼
Tasks Execute on Data Partitions
```

---

## Components of a Spark Application

### 1. Driver Program

The Driver is the brain of a Spark application.

Responsibilities:

- Creates the `SparkSession`
- Converts transformations into a DAG
- Optimizes execution plan
- Splits work into stages
- Schedules tasks
- Collects results from executors

Only **one Driver** exists per Spark application.

---

### 2. Cluster Manager

Responsible for allocating resources.

Examples:

- Standalone
- YARN
- Kubernetes
- Mesos

The Driver requests executors from the Cluster Manager.

---

### 3. Executors

Executors are JVM processes running on worker nodes.

Responsibilities:

- Execute tasks
- Store cached data
- Perform shuffle operations
- Send results back to the Driver

Each executor contains:

- Memory
- CPU Cores
- Task Slots

---

### 4. Tasks

A Task is the smallest unit of work in Spark.

- One task processes one partition.
- Tasks run in parallel across executors.

Example:

```
100 Partitions
      │
      ▼
100 Tasks
```

---

## Spark Execution Flow

```
User writes code

        │

Transformation
(map/filter/select)

        │

Lazy Evaluation

        │

Action
(show/count/collect)

        │

Driver creates Job

        │

Job split into Stages

        │

Stages split into Tasks

        │

Tasks sent to Executors

        │

Results returned to Driver
```

---

# Spark UI

Spark UI is a **web interface** used to monitor, debug, and analyze the performance of a Spark application.

Default URL:

```
http://localhost:4040
```

Each running Spark application gets its own Spark UI.

---

# Spark UI Tabs

## 1. Jobs Tab

Shows every Spark Job.

Information available:

- Job ID
- Status (Running / Succeeded / Failed)
- Duration
- Number of Stages
- DAG Visualization

Example:

```
Job 0

Status: Succeeded

Duration: 2.4 sec

Stages:
Stage 0
Stage 1
```

---

## 2. Stages Tab

Shows detailed information about each stage.

Metrics include:

- Number of Tasks
- Input Size
- Output Size
- Shuffle Read
- Shuffle Write
- Failed Tasks
- Execution Time

Useful for identifying slow stages.

---

## 3. Tasks

Each stage contains multiple tasks.

Spark UI shows:

- Task ID
- Executor ID
- Launch Time
- Finish Time
- Duration
- GC Time
- Input Records
- Shuffle Metrics

Example:

```
Stage 2

Task 0

Task 1

Task 2

Task 3
```

---

## 4. Storage Tab

Shows cached datasets.

Useful information:

- Cached DataFrames
- Storage Level
- Memory Used
- Disk Used
- Number of Cached Partitions

Example:

```
orders.cache()

appears here
```

---

## 5. Executors Tab

Displays executor information.

Metrics:

- Executor ID
- Host
- Memory Usage
- CPU Time
- Active Tasks
- Failed Tasks
- Total Shuffle Read
- Total Shuffle Write

Useful for detecting:

- Memory pressure
- Executor failures
- Data skew

---

## 6. SQL / DataFrame Tab

Available when using Spark SQL.

Shows:

- Physical Plan
- Logical Plan
- Query Execution
- SQL Metrics
- Operator Timeline

Useful for query optimization.

---

## 7. Environment Tab

Shows Spark configuration.

Examples:

```
spark.executor.memory

spark.executor.instances

spark.sql.shuffle.partitions

spark.driver.memory
```

Useful for verifying runtime configuration.

---

# Relationship Between Jobs, Stages, and Tasks

```
Spark Application

        │

      Job

        │

 ┌──────┴──────┐

Stage 0     Stage 1

 │ │ │         │ │

Task Task Task Task Task
```

- One application can have multiple jobs.
- One job can have multiple stages.
- One stage contains many tasks.
- One task processes one partition.

---

# What Spark UI Helps With

- Monitor running applications
- Analyze job execution
- Debug failed jobs
- Identify slow stages
- Detect data skew
- Monitor shuffle operations
- Check cache usage
- View executor resource utilization
- Inspect SQL query execution
- Verify Spark configuration

---

# Spark UI vs Driver/Executor Logs

| Spark UI | Driver/Executor Logs |
|----------|----------------------|
| Job execution details | INFO/WARN/ERROR logs |
| DAG visualization | Stack traces |
| Stage metrics | Exception details |
| Task metrics | Application logs |
| Shuffle statistics | Debug messages |
| Cache information | JVM logs |
| SQL execution metrics | Spark log output |

Spark UI focuses on **execution monitoring and performance analysis**, while Driver and Executor logs contain **application logs, warnings, errors, and stack traces**.

---

# Key Takeaways

- A Spark Application consists of a Driver, Cluster Manager, Executors, and Tasks.
- The Driver coordinates the entire application.
- Executors perform the actual computations.
- Spark UI helps monitor and analyze execution.
- Jobs are divided into stages, and stages are divided into tasks.
- One task processes one data partition.
- Spark UI is mainly used for performance monitoring, **not** for viewing application logs.