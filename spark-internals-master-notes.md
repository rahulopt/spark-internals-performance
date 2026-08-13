# Spark Internals & Performance — Master Notes
> Comprehensive reference covering all topics from the spark-internals-performance repo, with gaps filled and quality improved.

---

## Table of Contents

1. [Spark Application, SparkSession & Spark UI](#1-spark-application-sparksession--spark-ui)
2. [Spark Architecture — Driver, Executors, Cluster Manager](#2-spark-architecture)
3. [Jobs, Stages, Tasks & Execution Hierarchy](#3-jobs-stages-tasks--execution-hierarchy)
4. [Lazy Evaluation & DAG](#4-lazy-evaluation--dag)
5. [Partitions & Parallelism](#5-partitions--parallelism)
6. [Narrow vs Wide Transformations & Shuffle](#6-narrow-vs-wide-transformations--shuffle)
7. [Repartition vs Coalesce](#7-repartition-vs-coalesce)
8. [Join Strategies — All Types](#8-join-strategies)
9. [Memory Management — Unified Memory Model](#9-memory-management)
10. [Garbage Collection & JVM Tuning](#10-garbage-collection--jvm-tuning)
11. [Caching & Persistence](#11-caching--persistence)
12. [Data Skew & Salting](#12-data-skew--salting)
13. [Adaptive Query Execution (AQE)](#13-adaptive-query-execution-aqe)
14. [Bucketing](#14-bucketing)
15. [Broadcast Hash Join & Driver OOM](#15-broadcast-hash-join--driver-oom)
16. [Spark Submit & Deployment Modes](#16-spark-submit--deployment-modes)
17. [Serialization & Tungsten Engine](#17-serialization--tungsten-engine)
18. [Predicate Pushdown & Catalyst Optimizer](#18-predicate-pushdown--catalyst-optimizer)
19. [File Formats — Parquet, ORC, Avro, Delta](#19-file-formats)
20. [Performance Tuning Checklist](#20-performance-tuning-checklist)
21. [Production Troubleshooting Guide](#21-production-troubleshooting-guide)
22. [Interview Questions — All Topics](#22-interview-questions)

---

## 1. Spark Application, SparkSession & Spark UI

### What is a Spark Application?

A Spark Application is a complete program submitted to run on a Spark cluster. It consists of:
- **Driver Program** — coordinates the application
- **Cluster Manager** — allocates resources (YARN, Kubernetes, Standalone)
- **Executors** — JVM processes that run tasks on worker nodes
- **Tasks** — the smallest unit of work, each processes one partition

```
User Code
    │
    ▼
Driver Program
    ├── Creates SparkSession
    ├── Builds DAG
    ├── Optimizes Execution Plan
    └── Schedules Jobs/Stages/Tasks
          │
          ▼
    Cluster Manager
          │
          ▼
      Executors → Tasks → Process Partitions
```

### SparkSession

The unified entry point since Spark 2.0. It combines SparkContext, SQLContext, and HiveContext.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()
```

`SparkSession` provides access to:
- DataFrame / Dataset API
- Spark SQL
- SparkContext (via `spark.sparkContext`)
- Catalog (metadata)
- Streaming

### Spark UI

Default URL: `http://localhost:4040`

Each running application gets its own Spark UI.

| Tab | What it shows | What to look for |
|-----|--------------|-----------------|
| Jobs | All Spark jobs, status, duration | Failed jobs, unexpectedly long jobs |
| Stages | Per-stage metrics: tasks, shuffle read/write, spill | Slow stages, data skew |
| Tasks | Per-task metrics: duration, GC time, records | Straggler tasks (skew indicator) |
| Storage | Cached DataFrames, storage level, memory/disk used | Cache eviction, over-caching |
| Executors | Memory usage, GC time, shuffle stats per executor | Memory pressure, dead executors |
| SQL/DataFrame | Physical plan, query timeline, SQL metrics | Expensive operators, missing pushdown |
| Environment | Spark config, JVM version, classpath | Verify config applied correctly |

**Key Spark UI patterns to recognize:**

- **Data skew**: One task takes 10x longer than others in the same stage
- **Spill to disk**: Look for "Spill (Memory)" and "Spill (Disk)" columns > 0
- **Shuffle overhead**: Large "Shuffle Read/Write" bytes → consider bucketing
- **GC pressure**: High GC Time in executor tab → tune memory or reduce object creation

---

## 2. Spark Architecture

### High-Level Architecture

```
Spark Application
    │
    ├── Driver (1 per app)
    │       ├── SparkSession / SparkContext
    │       ├── DAG Scheduler
    │       ├── Task Scheduler
    │       └── Block Manager (driver side)
    │
    ├── Cluster Manager (YARN / Kubernetes / Standalone)
    │       └── Resource Allocation
    │
    └── Executors (N per app)
            ├── Task Threads (= num cores)
            ├── Block Manager (cached data)
            └── Shuffle Service
```

### Driver Responsibilities

| Responsibility | Detail |
|---|---|
| SparkSession creation | Entry point for all Spark operations |
| Logical plan building | Records all transformations as a DAG |
| Query optimization | Catalyst optimizer rewrites the plan |
| Job/Stage/Task scheduling | DAGScheduler and TaskScheduler |
| Result collection | `collect()`, `show()` bring data to driver |
| Metadata management | Tracks partition locations, broadcast vars |

> **Critical**: The driver does NOT process data. It only coordinates. Processing happens on executors.

### Executor Responsibilities

- Execute tasks assigned by the driver
- Store data in memory for caching (`BlockManager`)
- Perform shuffle write (map side) and shuffle read (reduce side)
- Report metrics and heartbeats back to driver

### Driver vs Executor

| | Driver | Executor |
|--|--------|----------|
| Count | 1 per app | Multiple per app |
| Role | Coordinator | Worker |
| Runs | User code, scheduling | Tasks |
| Memory issue cause | `collect()`, large metadata | Join, aggregation, cache |
| OOM symptom | `Driver OOM` | `ExecutorLostFailure`, task OOM |

### Cluster Modes Summary

| Mode | Driver runs on | Use case |
|------|---------------|----------|
| Client mode | Machine submitting the job | Interactive (notebooks, dev) |
| Cluster mode | Inside cluster (worker node) | Production batch jobs |
| Local mode | Single JVM | Development, testing |

### Worker Node vs Executor

- **Worker Node** = physical or virtual machine
- **Executor** = JVM process running on a worker node

One worker node can host multiple executors. They are not the same thing.

---

## 3. Jobs, Stages, Tasks & Execution Hierarchy

### Execution Hierarchy

```
Application
    └── Job (triggered by each Action)
            └── Stage (separated by shuffle boundaries)
                    └── Task (1 per partition per stage)
                            └── Processes 1 Partition
```

### How a Job is Created

Every **action** triggers a job. Transformations are lazy — they only build the plan.

```python
# No job created — lazy transformations
df2 = df.filter(df["amount"] > 100)
df3 = df2.groupBy("customer_id").sum("amount")

# Job created here — this is the action
df3.show()   # Job 1
df3.count()  # Job 2
```

### How Stages are Created

Stages are separated at **shuffle boundaries** (wide transformations).

```
filter() → select()          →  Stage 0  (narrow, no shuffle)
                    ↓ Shuffle
groupBy() → aggregate()      →  Stage 1  (after shuffle)
```

- Narrow transformations within a stage are **pipelined** (no intermediate write)
- Each shuffle creates a new stage

### How Tasks are Created

Within a stage: **1 task per partition**

```
Stage 0 has 8 partitions → 8 Tasks
Tasks run in parallel up to available executor cores
```

If you have 4 executor cores total:
```
Wave 1: Task 0, 1, 2, 3 (parallel)
Wave 2: Task 4, 5, 6, 7 (parallel)
```

### Task Execution Lifecycle

```
Driver sends task → Executor deserializes task
                 → Reads partition data
                 → Applies transformations
                 → Writes shuffle output (if wide transform follows)
                 → Reports metrics + completion to Driver
```

---

## 4. Lazy Evaluation & DAG

### What is Lazy Evaluation?

Spark does not execute transformations immediately. It builds a **logical plan** and only executes when an action is called.

```python
# Nothing executed yet — just builds DAG
df2 = df.filter("amount > 100")          # Step 1 recorded
df3 = df2.select("customer_id", "amount") # Step 2 recorded
df4 = df3.groupBy("customer_id").sum()   # Step 3 recorded

# NOW Spark executes everything optimally
df4.show()
```

### Why Lazy Evaluation?

- Allows **Catalyst optimizer** to see the entire plan and reorder/optimize
- Enables **predicate pushdown** (filters pushed to data source)
- Enables **column pruning** (unused columns never loaded)
- Avoids unnecessary computation if result is never needed

### DAG (Directed Acyclic Graph)

The DAG is Spark's representation of the computation pipeline.

```
Read CSV
    │
    ▼
Filter (amount > 100)
    │
    ▼
Select (customer_id, amount)
    │
    ▼
GroupBy customer_id       ← Wide transformation = shuffle boundary
    │
    ▼
Sum(amount)
    │
    ▼
Show (Action)
```

- **Directed**: transformations flow in one direction
- **Acyclic**: no cycles — each step builds on previous
- **Graph**: multiple branches and dependencies possible

### Lineage

The DAG also serves as **lineage** — Spark knows how to recompute any lost partition by replaying transformations from the source. This is how Spark achieves fault tolerance without replication.

---

## 5. Partitions & Parallelism

### What is a Partition?

A partition is the smallest unit of data Spark processes. One task processes exactly one partition within a stage.

```
10 GB file → 80 partitions (at 128MB each) → 80 tasks
```

### How Partitions are Created

| Source | Default behavior |
|--------|-----------------|
| CSV/JSON/Text file | One partition per HDFS/S3 block (default 128MB) |
| Parquet file | Based on file size and row group size |
| After shuffle (groupBy, join) | Controlled by `spark.sql.shuffle.partitions` (default 200) |
| `repartition(n)` | Exactly n partitions via full shuffle |
| `coalesce(n)` | Reduced to n without full shuffle |

### Checking Partitions

```python
df.rdd.getNumPartitions()  # count of partitions

# Check partition sizes (approximate)
df.groupBy(spark_partition_id()).count().show()
```

### Parallelism

```
Available parallelism = num_executors × cores_per_executor

Example:
  5 executors × 4 cores = 20 task slots

  With 100 partitions:
    Wave 1: 20 tasks
    Wave 2: 20 tasks
    ...
    Wave 5: 20 tasks
```

### Optimal Partition Count

| Situation | Target |
|-----------|--------|
| General rule | 2x–4x total executor cores |
| Partition size | 128MB–256MB per partition |
| After shuffle | Set `spark.sql.shuffle.partitions` to ~2x cores |
| Task duration | Aim for 100ms–5s per task |

**Too few partitions** → underutilized cores, OOM risk
**Too many partitions** → scheduling overhead, tiny file problem

```python
# Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "80")

# Or let AQE handle it (Spark 3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## 6. Narrow vs Wide Transformations & Shuffle

### Narrow Transformations

Each output partition depends on **at most one** input partition. No data movement across partitions needed.

| Operation | Example |
|-----------|---------|
| `filter` | `df.filter(col("age") > 30)` |
| `select` | `df.select("name", "age")` |
| `withColumn` | `df.withColumn("tax", col("price") * 0.1)` |
| `map` / `flatMap` | RDD transformations |
| `union` | Combines two DataFrames |

```
Partition 0 → filter → Partition 0'
Partition 1 → filter → Partition 1'
Partition 2 → filter → Partition 2'
(each processes independently — no network I/O)
```

### Wide Transformations

Each output partition may depend on **many** input partitions. Requires **shuffle** — data moves across the network.

| Operation | Why it shuffles |
|-----------|----------------|
| `groupBy` | Same keys must land in same partition |
| `join` | Matching keys must meet |
| `distinct` | Duplicates could be in any partition |
| `orderBy` | Global sort requires all data visible |
| `repartition` | Redistributes data |
| `window functions` | Requires data by partition key |

### Shuffle Internals

```
Map Phase (Stage N):
  Each task reads its partition
  Partitions output data by hash(key) % num_reduce_partitions
  Writes shuffle files to local disk

Network Transfer:
  Shuffle files transferred across network

Reduce Phase (Stage N+1):
  Each task reads its assigned key range from all map outputs
  Merges and processes
```

**Shuffle is the most expensive operation in Spark:**
- Writes data to disk
- Transfers data over network
- Creates new partitions on the other side

### Reducing Shuffle Cost

```python
# 1. Filter before shuffle
df.filter(col("date") > "2024-01-01").groupBy("customer_id").count()

# 2. Use bucketing for repeated joins on same key (avoids shuffle entirely)
# See bucketing section

# 3. Use broadcast join for small tables (avoids shuffle entirely)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# 4. Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "100")

# 5. Enable AQE to auto-coalesce shuffle partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## 7. Repartition vs Coalesce

### repartition(n)

- Performs a **full shuffle** — all data redistributed across network
- Can **increase or decrease** partition count
- Produces **evenly distributed** partitions (hash-based)
- Use when you need balanced partitions for parallel processing

```python
df = df.repartition(100)              # redistribute to 100 balanced partitions
df = df.repartition(100, "region")   # partition by column value (co-location)
```

### coalesce(n)

- **No full shuffle** — merges existing partitions locally
- Can only **decrease** partition count
- May produce **uneven** partitions (some are merged, others not)
- Use when reducing partitions before writing output

```python
df = df.coalesce(10)   # reduce to 10 partitions, minimal network I/O
```

### Comparison Table

| | repartition(n) | coalesce(n) |
|--|---------------|-------------|
| Shuffle | Full shuffle | No shuffle (or minimal) |
| Direction | Increase or decrease | Decrease only |
| Balance | Even | Potentially uneven |
| Cost | Expensive | Cheap |
| Best for | Increasing parallelism, fixing skew | Reducing files before write |
| Stage boundary | Yes (new stage) | No (same stage) |

### When to Use Which

```python
# Before a large join — balance partitions
df = df.repartition(200)

# Before writing to S3 — reduce files
df.coalesce(10).write.parquet("s3://bucket/output/")

# Fix data skew — repartition by different key
df = df.repartition(100, "evenly_distributed_col")
```

> **Tip**: `coalesce(1)` before writing creates a single output file — useful for small results but causes bottleneck on large data.

---

## 8. Join Strategies

Spark has 5 physical join strategies. Understanding which one Spark chooses (and why) is critical for performance.

### 8.1 Broadcast Hash Join (BHJ)

**Best for: large table JOIN small table (< 10MB by default)**

The small table is broadcast (copied) to all executors. No shuffle needed.

```
Driver collects small table
    │
    ▼
Broadcasts to all Executors
    │
Each executor has a hash table of the small table in memory
    │
Large table partition → local hash lookup → result
```

```python
from pyspark.sql.functions import broadcast

result = large_df.join(broadcast(small_df), "customer_id")
```

**Configuration:**
```python
# Threshold below which Spark auto-broadcasts (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# Disable broadcast entirely
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

**When it fails (Driver OOM):**
- Table is larger than estimated (stats outdated)
- Multiple broadcast joins → driver accumulates too much data
- Fix: reduce threshold, or explicitly avoid broadcast for large tables

---

### 8.2 Sort-Merge Join (SMJ)

**Best for: two large tables on the same key**

Both tables are sorted and shuffled by join key, then merged.

```
Table A partitions          Table B partitions
    │                           │
    ▼ shuffle by key            ▼ shuffle by key
Sorted partition A0         Sorted partition B0
    │                           │
    └─────── merge join ────────┘
                 │
              Result
```

```
Step 1: Shuffle both tables by join key
Step 2: Sort each partition by join key
Step 3: Merge-scan matching partitions (like merge in merge sort)
```

**Characteristics:**
- Requires two shuffles (expensive but scalable)
- Memory efficient — no large hash tables needed
- Default strategy for large-large joins in Spark

**Detect in explain():**
```
SortMergeJoin [customer_id], [customer_id]
```

---

### 8.3 Shuffle Hash Join (SHJ)

**Best for: medium-large + small-medium table, when one side fits in memory per partition**

One table is shuffled and used to build a hash table per partition. The other side probes it.

```
Build side (smaller): shuffle → hash table per partition
Probe side (larger):  shuffle → probe hash table
```

```python
# Hint Spark to use shuffle hash
from pyspark.sql.functions import expr
result = large_df.join(medium_df.hint("shuffle_hash"), "key")
```

**Difference from SMJ:**
- No sort step → faster when data fits in memory
- Requires build-side partition to fit in executor memory
- Risky with skewed data (one partition might OOM)

---

### 8.4 Broadcast Nested Loop Join (BNLJ)

**Used for: non-equi joins (range conditions, cross joins)**

Inefficient O(n×m) nested loop. Avoid for large datasets.

```python
# Cross join example
result = df1.crossJoin(df2)

# Non-equi join
result = df1.join(df2, df1["price"] > df2["min_price"])
```

---

### 8.5 Cartesian Product Join

Produces every combination of rows. Only valid with `crossJoin()` explicitly.

```python
spark.conf.set("spark.sql.crossJoin.enabled", "true")
result = df1.crossJoin(df2)
```

---

### Join Strategy Decision Tree

```
Is one table small (< autoBroadcastJoinThreshold)?
    YES → Broadcast Hash Join (fastest)
    NO  →
        Is data bucketed on join key?
            YES → Bucket merge join (no shuffle!)
            NO  →
                Can build side fit per partition in memory?
                    YES → Shuffle Hash Join
                    NO  → Sort-Merge Join (most scalable)
```

### Join Hints

```python
df1.join(df2.hint("broadcast"), "key")      # force BHJ
df1.join(df2.hint("shuffle_hash"), "key")   # force SHJ
df1.join(df2.hint("merge"), "key")          # force SMJ
df1.join(df2.hint("shuffle_replicate_nl"), "key")  # force BNLJ
```

---

## 9. Memory Management

### Executor Memory Model

```
Executor Container Memory
    │
    ├── JVM Heap (--executor-memory)
    │       │
    │       ├── Reserved Memory (~300MB, Spark internal)
    │       │
    │       └── Unified Memory (spark.memory.fraction × remaining heap)
    │               │
    │               ├── Execution Memory (join, sort, agg, shuffle)
    │               └── Storage Memory (cache, persist, broadcast)
    │
    └── Off-Heap / Memory Overhead
            ├── JVM native memory
            ├── Python worker (PySpark)
            ├── Off-heap storage (if enabled)
            └── Internal Spark overhead
```

### Key Configurations

```python
# Executor heap memory
--executor-memory 8g

# Memory overhead (container memory above JVM heap)
# Default: max(384MB, 0.1 × executor-memory)
spark.executor.memoryOverhead = 2g

# Fraction of heap for unified memory (default 0.6)
spark.memory.fraction = 0.6

# Fraction of unified memory initially for storage (default 0.5)
spark.memory.storageFraction = 0.5

# Total container memory = executor-memory + memoryOverhead
```

### Unified Memory — Dynamic Borrowing

Execution and storage can borrow from each other:

```
Unified Memory = 6 GB (60% of 10GB heap)

Normal state:
  Execution: 3 GB
  Storage:   3 GB

Heavy aggregation needs more execution:
  Execution: 5 GB  (borrowed 2GB from storage)
  Storage:   1 GB  (cached partitions may be evicted)
```

**Important**: Storage memory eviction is LRU-based. Evicted cached partitions are recomputed from lineage on next access.

### Memory Regions and Their Use

| Region | Used for | Problem when full |
|--------|----------|-------------------|
| Execution Memory | Sort, hash tables for joins/agg, shuffle buffer | Spill to disk |
| Storage Memory | `cache()`, `persist()`, broadcast vars | Cache eviction |
| Memory Overhead | Python workers, native libs, off-heap | Container killed by YARN/K8s |

### Disk Spill

When execution memory is insufficient, Spark spills intermediate data to disk:

```
Sort operation
    │
    ├── Memory fits → stays in memory → fast
    └── Memory full → spill to disk → read back → much slower
```

**Detect spill**: In Spark UI Stages tab, look for "Spill (Memory)" and "Spill (Disk)" columns.

**Fix spill:**
```python
# Increase executor memory
--executor-memory 16g

# Reduce partition size (more partitions = less data per task)
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Increase memory fraction
spark.conf.set("spark.memory.fraction", "0.7")
```

### PySpark Memory Architecture

```
Executor JVM
    └── Spark Executor (Java)
            └── Python Worker (subprocess)
                    └── Your Python code (pandas, numpy, etc.)
```

PySpark tasks serialize data between JVM and Python via pipes. This adds:
- Memory overhead for Python process
- Serialization/deserialization cost
- Extra memory usage (data in both JVM and Python)

```python
# Increase for PySpark workloads
spark.executor.memoryOverhead = 4g
# or
spark.executor.pyspark.memory = 2g  # dedicated Python worker memory (Spark 3+)
```

---

## 10. Garbage Collection & JVM Tuning

### Why GC Matters in Spark

Spark runs on the JVM. The garbage collector reclaims unused heap memory. Frequent or long GC pauses:
- Pause all application threads
- Cause task timeouts
- Reduce throughput by 20–50% in extreme cases

### GC in Spark UI

Check the **Executors** tab → **GC Time** column.

- GC Time < 5% of task time: healthy
- GC Time 10–20%: worth investigating
- GC Time > 20%: problematic — tune memory or code

### Types of GC

| Type | Description | Cost |
|------|-------------|------|
| Minor GC | Collects young generation (short-lived objects) | Fast (ms) |
| Major GC | Collects old generation (long-lived objects) | Slow (100ms–seconds) |
| Full GC | Collects entire heap | Very slow, stops the world |

### What Causes Excessive GC in Spark?

1. **Too many short-lived objects** — e.g., creating new objects per row
2. **Too much data in heap** — cached DataFrames filling storage memory
3. **Small executor heap** — frequent minor GC and premature promotion
4. **Python UDFs** — trigger extra serialization and object creation

### GC Tuning Strategies

**1. Use G1GC (recommended for Spark)**
```bash
spark.executor.extraJavaOptions=-XX:+UseG1GC \
  -XX:G1HeapRegionSize=32m \
  -XX:InitiatingHeapOccupancyPercent=35
```

**2. Reduce storage memory fraction (more heap for execution → fewer promoted objects)**
```python
spark.conf.set("spark.memory.storageFraction", "0.3")
```

**3. Avoid Python UDFs — use native Spark functions instead**
```python
# BAD: UDF creates Python objects, serializes across JVM boundary
from pyspark.sql.functions import udf
@udf("double")
def tax_calc(price):
    return price * 0.18

# GOOD: native Spark function stays in JVM, no serialization
from pyspark.sql.functions import col
df.withColumn("tax", col("price") * 0.18)
```

**4. Use Pandas UDFs (Arrow-based) when UDF is unavoidable**
```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def tax_pandas_udf(series: pd.Series) -> pd.Series:
    return series * 0.18
```

**5. Unpersist cached DataFrames when no longer needed**
```python
df.cache()
# ... use df ...
df.unpersist()  # free storage memory
```

**6. Enable off-heap storage**
```python
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```
Stores cached data outside JVM heap → less GC pressure.

---

## 11. Caching & Persistence

### cache() vs persist()

```python
df.cache()              # = persist(StorageLevel.MEMORY_AND_DISK)
df.persist()            # default = MEMORY_AND_DISK
df.persist(StorageLevel.MEMORY_ONLY)
```

`cache()` is shorthand for the default persistence level. Use `persist()` when you need to specify a different storage level.

### Storage Levels

| Level | Memory | Disk | Serialized | Replicated | Use case |
|-------|--------|------|-----------|-----------|---------|
| `MEMORY_ONLY` | ✅ | ❌ | ❌ | ❌ | Fast access, data fits in memory |
| `MEMORY_AND_DISK` | ✅ | ✅ | ❌ | ❌ | Default — safe fallback to disk |
| `MEMORY_ONLY_SER` | ✅ | ❌ | ✅ | ❌ | Less memory, slower deserialization |
| `MEMORY_AND_DISK_SER` | ✅ | ✅ | ✅ | ❌ | Low memory, slow access |
| `DISK_ONLY` | ❌ | ✅ | ✅ | ❌ | Very large data, infrequently reused |
| `MEMORY_ONLY_2` | ✅ | ❌ | ❌ | ✅ | Fault tolerant, double memory cost |
| `OFF_HEAP` | Off-heap | ❌ | ✅ | ❌ | Reduce GC pressure |

### When to Cache

**Good candidates:**
- DataFrame used in multiple actions/jobs
- DataFrame that took expensive computation (large join, window function)
- Iterative algorithms (ML training loops)

**Bad candidates:**
- DataFrame used only once
- Very large DataFrame that won't fit in memory
- DataFrame read from fast source (columnar Parquet on SSD/S3)

### Cache is Lazy

```python
df.cache()      # does NOT cache yet — just marks DataFrame
df.count()      # THIS triggers caching — data is read and stored
df.show()       # reads from cache — fast
```

### Unpersist

```python
df.unpersist()              # remove from cache
df.unpersist(blocking=True) # wait for eviction to complete
```

Always unpersist when done. Spark will auto-evict LRU partitions if memory is needed, but explicit unpersist is cleaner.

### Cache vs Checkpoint

| | cache() | checkpoint() |
|--|---------|-------------|
| Stores in | Memory/disk (executor) | HDFS/S3 (reliable storage) |
| Lineage | Preserved | Truncated |
| Fault tolerant | Limited | Yes (data on HDFS) |
| Use case | Performance | Breaking long lineage chains, ML |

```python
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")
df.checkpoint()  # triggers computation + saves to HDFS
```

---

## 12. Data Skew & Salting

### What is Data Skew?

Data skew occurs when some partitions contain far more data than others. A few tasks take much longer than the rest, creating a bottleneck.

```
Stage 5 — 100 tasks
  Task 0–98:  2 seconds each   ← fast
  Task 99:    45 minutes       ← skewed! one hot key
```

**Symptom in Spark UI**: In the Stages tab, task durations show one or a few tasks taking 10x–100x longer than median.

### Causes of Skew

- **Highly frequent key**: e.g., `customer_id = "AMAZON"` has 80% of all orders
- **Null keys**: all nulls hash to same partition in a join
- **Date skew**: most records have the same `event_date`

### Detecting Skew

```python
# Check key distribution before joining
df.groupBy("join_key").count().orderBy("count", ascending=False).show(20)

# Check partition sizes
from pyspark.sql.functions import spark_partition_id
df.groupBy(spark_partition_id()).count().show()
```

### Fix 1: Salting (Most Common)

Add a random salt to the join key to spread one hot key across multiple partitions.

```python
from pyspark.sql.functions import col, concat, lit, rand, floor
import math

SALT_FACTOR = 10  # spread hot keys across 10 partitions

# Salt the large (skewed) table
large_df = large_df.withColumn(
    "salted_key",
    concat(col("join_key"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Explode the small (dimension) table to match all salted versions
from pyspark.sql.functions import explode, array
small_df = small_df.withColumn(
    "salt_range",
    array([lit(i) for i in range(SALT_FACTOR)])
).withColumn("salt", explode(col("salt_range"))) \
 .withColumn(
    "salted_key",
    concat(col("join_key"), lit("_"), col("salt").cast("string"))
 ).drop("salt_range", "salt")

# Join on salted key
result = large_df.join(small_df, "salted_key").drop("salted_key")
```

### Fix 2: AQE Skew Join (Spark 3+, Automatic)

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# AQE automatically splits skewed partitions during shuffle joins
# No code change needed!
```

### Fix 3: Filter and Union (for null skew)

```python
# Separate null keys — process independently
non_null = df.filter(col("key").isNotNull())
null_rows = df.filter(col("key").isNull())

result_non_null = non_null.join(other_df, "key")
result_final = result_non_null.union(null_rows)
```

### Fix 4: Repartition by a Better Key

```python
# If skew is on one column, repartition by a composite key
df = df.repartition(100, "region", "date")  # more uniform distribution
```

### Fix 5: Broadcast the Skewed Table's Dimension

If the table causing skew is small enough:
```python
result = large_df.join(broadcast(small_df), "key")
# No shuffle → no skew impact
```

---

## 13. Adaptive Query Execution (AQE)

### What is AQE?

Introduced in Spark 3.0. AQE dynamically re-optimizes query execution **at runtime**, using actual statistics collected after each shuffle stage.

Traditional Spark: plan once at the start → execute (static plan)
AQE: plan → execute → collect stats → re-plan → execute → collect → ... (adaptive)

```python
# Enable AQE (default enabled in Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### AQE Feature 1: Coalescing Shuffle Partitions

After a shuffle, AQE merges small partitions into larger ones.

```
Before shuffle: 200 partitions configured
After shuffle data is small (e.g., 1MB total):
  → AQE coalesces to 5 partitions automatically

Avoids 195 tiny tasks with no work to do
```

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
```

### AQE Feature 2: Skew Join Optimization

AQE detects skewed partitions after the shuffle and automatically splits them.

```
Shuffle produces:
  Partition 0: 100MB  ← normal
  Partition 1: 100MB  ← normal
  Partition 2:   2GB  ← skewed!

AQE splits partition 2 into sub-partitions:
  Partition 2a: 500MB
  Partition 2b: 500MB
  Partition 2c: 500MB
  Partition 2d: 500MB

Duplicates the matching rows from the other table to handle sub-partitions
```

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # 5x median = skewed
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
```

### AQE Feature 3: Dynamic Join Strategy Switching

AQE can change a Sort-Merge Join to a Broadcast Hash Join at runtime if the post-shuffle data turns out to be small enough.

```
Plan time: both tables look large → plan Sort-Merge Join
Runtime:   after filtering, one table is only 5MB → switch to Broadcast Join
```

```python
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### AQE Configuration Summary

```python
# Master switch
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Partition coalescing
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")

# Skew join
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# Dynamic join switching
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

### When AQE Doesn't Help

- Streaming jobs (no batch statistics)
- When plan-time statistics are accurate (AQE has no new info to act on)
- When you need deterministic partition counts (AQE changes them dynamically)

---

## 14. Bucketing

### What is Bucketing?

Bucketing pre-sorts and pre-partitions data by a key when writing to disk. Subsequent joins on that key can skip the shuffle entirely.

```
Without bucketing:
  Join → shuffle both tables → merge → result
  Cost: 2 shuffles

With bucketing (both tables bucketed on same key, same bucket count):
  Join → read pre-bucketed files → merge → result
  Cost: 0 shuffles
```

### How Bucketing Works

```python
# Write with bucketing
orders_df.write \
    .bucketBy(64, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("orders_bucketed")

customers_df.write \
    .bucketBy(64, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("customers_bucketed")

# Join — Spark skips shuffle if bucket counts match
orders = spark.table("orders_bucketed")
customers = spark.table("customers_bucketed")
result = orders.join(customers, "customer_id")
```

### Requirements for Shuffle-Free Join

- Both tables bucketed on the **same column(s)**
- Both tables have the **same bucket count**
- Both tables stored in a Spark-managed table (Hive metastore)

### Bucketing vs Partitioning

| | Bucketing | Partitioning |
|--|-----------|-------------|
| Based on | Hash of column | Column value (directory) |
| Eliminates shuffle | Yes (for matching joins) | No |
| Predicate filter | Not directly | Yes (partition pruning) |
| Use case | Repeated joins on same key | Date/region-based filtering |
| Storage | Multiple files per bucket | Directory per value |

```python
# Combining both
df.write \
    .partitionBy("date") \
    .bucketBy(64, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("orders_partitioned_bucketed")
```

### Verify Bucketing Works

```python
orders.join(customers, "customer_id").explain()
# Look for: Exchange missing → shuffle was skipped
# If you see: Exchange hashpartitioning → bucketing didn't work
```

---

## 15. Broadcast Hash Join & Driver OOM

### Broadcast Hash Join Deep Dive

When one table is small enough, Spark broadcasts it to all executors:

```
Step 1: Driver collects small table from executors
Step 2: Driver serializes it and sends to ALL executors
Step 3: Each executor stores hash table in memory
Step 4: Large table partition does local hash lookup — no shuffle

Threshold: spark.sql.autoBroadcastJoinThreshold = 10MB (default)
```

**Force broadcast:**
```python
from pyspark.sql.functions import broadcast
result = large.join(broadcast(small), "id")
```

**Disable auto-broadcast:**
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Driver OOM from Broadcast Join

The broadcast table passes through the **driver** before going to executors:

```
Executor 1 ─┐
Executor 2 ─┤── sends table data ──► Driver collects it ──► broadcasts to all executors
Executor 3 ─┘
```

If the table is large (e.g., 1GB with poor stats), the driver can run out of memory:

```
java.lang.OutOfMemoryError: Java heap space (in Driver)
```

**Fix:**
```python
# Lower broadcast threshold to avoid accidentally broadcasting large tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")

# Increase driver memory
--driver-memory 8g

# Or disable broadcast for a specific join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
result = large.join(medium, "id")  # will use Sort-Merge Join
```

### Other Driver OOM Causes

| Cause | Fix |
|-------|-----|
| `df.collect()` on large data | Use `df.limit(n).collect()` or write to storage |
| `df.toPandas()` on large data | Filter/aggregate first, then convert |
| Large broadcast table | Lower threshold, increase driver memory |
| Many cached small results | Clear driver-side caches |
| Large accumulators | Use aggregated metrics, not per-row |

```python
# DANGEROUS — pulls ALL data to driver
result = df.collect()

# SAFER — limit first
result = df.limit(1000).collect()

# BEST — write directly to storage
df.write.parquet("s3://bucket/output/")
```

---

## 16. Spark Submit & Deployment Modes

### spark-submit Syntax

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.adaptive.enabled=true \
  --py-files dependencies.zip \
  --files config.json \
  my_script.py \
  --arg1 value1
```

### Deployment Modes

**Client Mode:**
```bash
--deploy-mode client
```
```
Your machine (client)
    └── Driver runs HERE (on your machine)
            └── Requests executors from cluster
```
- Driver stdout/stderr visible in your terminal
- Driver dies if client machine disconnects
- Best for: interactive jobs, Jupyter notebooks, debugging

**Cluster Mode:**
```bash
--deploy-mode cluster
```
```
Cluster
    ├── Application Master / Driver (runs inside cluster)
    └── Executors
```
- Driver runs inside cluster → survives client disconnection
- Logs go to cluster log aggregation (YARN logs, CloudWatch)
- Best for: production batch jobs

**Local Mode:**
```bash
--master local       # 1 thread
--master local[4]    # 4 threads
--master local[*]    # use all CPU cores
```
- Everything runs in single JVM
- Good for development, unit testing

### Key spark-submit Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--master` | Cluster manager URL | `yarn`, `local[*]`, `k8s://...` |
| `--deploy-mode` | Driver location | `client`, `cluster` |
| `--driver-memory` | Driver heap | `4g` |
| `--executor-memory` | Executor heap | `8g` |
| `--executor-cores` | Cores per executor | `4` |
| `--num-executors` | Number of executors | `10` |
| `--conf` | Spark config key=value | `spark.sql.adaptive.enabled=true` |
| `--py-files` | Python dependencies | `deps.zip` |
| `--files` | Files to ship to executors | `config.json` |
| `--jars` | JARs to add to classpath | `lib.jar` |
| `--packages` | Maven packages | `org.apache.spark:...` |

### Dynamic Resource Allocation

Instead of fixed `--num-executors`, let Spark scale up/down:

```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
```

### EMR Serverless Submit

```bash
aws emr-serverless start-job-run \
  --application-id <app-id> \
  --execution-role-arn <role-arn> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://bucket/script.py",
      "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g"
    }
  }'
```

---

## 17. Serialization & Tungsten Engine

### Serialization in Spark

When data moves between JVM and Python (PySpark), or over the network (shuffle), it must be serialized.

**Default serialization options:**

| Serializer | Description | Performance |
|------------|-------------|-------------|
| Java Serializer | Default for RDD | Slow, verbose |
| Kryo Serializer | Faster, more compact | Better for RDD workloads |
| Apache Arrow | Used by Pandas UDFs | Very fast, columnar |
| Tungsten (internal) | Spark's own binary format | Fastest — used internally |

**Enable Kryo for RDD workloads:**
```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

# Register custom classes for better performance
spark.conf.set("spark.kryo.classesToRegister", "com.example.MyClass")
```

**Note**: DataFrame/Dataset API (Spark SQL) uses Tungsten internally — Kryo setting doesn't apply here.

### Apache Arrow (PySpark Optimization)

Arrow enables fast, zero-copy transfer between JVM and Python:

```python
# Enable Arrow for toPandas() and createDataFrame()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

# Now toPandas() is much faster
pandas_df = spark_df.toPandas()
```

### Tungsten Engine

Tungsten is Spark's internal execution engine (since Spark 1.5). It operates on **binary data directly in memory** rather than JVM objects.

**What Tungsten does:**

1. **Off-heap memory management** — manages its own memory pages, avoids JVM object overhead and GC
2. **Cache-aware computation** — stores data in cache-friendly layouts
3. **Code generation (Whole-Stage CodeGen)** — generates specialized Java bytecode for each query rather than interpreting operators row by row
4. **Vectorized execution** — processes batches of rows (columnar) rather than one row at a time

**Whole-Stage CodeGen:**
```python
# Spark generates Java code like this for filter + project:
# while (inputIterator.hasNext()) {
#   InternalRow row = inputIterator.next();
#   if (row.getInt(0) > 100) {
#     output.append(row.getLong(1));
#   }
# }
# Instead of calling separate filter() and select() operators in a loop

df.explain()  # Look for "*(1) Project" — * means codegen is applied
```

**Check codegen is working:**
```python
df.explain()
# *(1) HashAggregate — codegen applied
# (1) Exchange        — no codegen (shuffle boundary)
```

**Disable for debugging:**
```python
spark.conf.set("spark.sql.codegen.wholeStage", "false")
```

---

## 18. Predicate Pushdown & Catalyst Optimizer

### Catalyst Optimizer

Catalyst is Spark SQL's query optimizer. It rewrites your logical plan into a more efficient physical plan through several passes:

```
User DataFrame Code
        │
        ▼
Unresolved Logical Plan   (column names not validated)
        │
        ▼  Analysis
Resolved Logical Plan     (columns/types validated)
        │
        ▼  Optimization (Catalyst rules applied)
Optimized Logical Plan    (predicates pushed, columns pruned, etc.)
        │
        ▼  Physical Planning
Physical Plans            (multiple candidates)
        │
        ▼  Cost Model
Best Physical Plan
        │
        ▼  Code Generation (Tungsten)
Executable RDDs
```

### Predicate Pushdown

Push filter conditions as close to the data source as possible — ideally to the file reader level, so unnecessary rows are never read into memory.

```python
# You write this:
df = spark.read.parquet("s3://bucket/orders/")
result = df.filter(col("date") >= "2024-01-01").select("customer_id", "amount")

# Catalyst pushes the filter DOWN to the Parquet reader:
# Instead of: read all data → filter → project
# Spark does: read only rows where date >= 2024-01-01, only load 2 columns
```

**Verify predicate pushdown:**
```python
df.filter(col("date") >= "2024-01-01").explain()

# Look for: PushedFilters: [IsNotNull(date), GreaterThanOrEqual(date, 2024-01-01)]
# If PushedFilters is empty → pushdown didn't work
```

### Column Pruning

Spark only reads columns that are actually used:

```python
# Only "name" and "age" columns are read from Parquet
df.select("name", "age").show()

# Verify: explain() shows Project [name, age] before FileScan
```

### Partition Pruning

For partitioned tables, Spark only reads relevant partitions:

```python
# Table partitioned by "date"
df = spark.read.parquet("s3://bucket/orders/")
result = df.filter(col("date") == "2024-06-01")

# Spark only reads: s3://bucket/orders/date=2024-06-01/
# All other date partitions are skipped
```

**Dynamic partition pruning (Spark 3+):**
```python
# Even in joins, Spark can prune partitions based on join results
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
```

### Catalyst Optimization Rules Examples

| Rule | What it does |
|------|-------------|
| Predicate Pushdown | Move filters closer to source |
| Column Pruning | Only load needed columns |
| Constant Folding | `1 + 2` → `3` at plan time |
| Null Propagation | Simplify nullability expressions |
| Boolean Simplification | `x == true` → `x` |
| Join Reordering | Smaller tables first in multi-join |
| Subquery Elimination | Remove redundant subqueries |

### When Pushdown Doesn't Work

```python
# UDFs block pushdown — Catalyst can't look inside Python functions
@udf("boolean")
def my_filter(x):
    return x > 100

df.filter(my_filter(col("amount")))  # NO pushdown — filter stays in Spark

# Fix: use native Spark expression
df.filter(col("amount") > 100)       # pushdown works
```

---

## 19. File Formats

### Comparison Table

| Format | Encoding | Splittable | Schema | Compression | Best for |
|--------|----------|-----------|--------|-------------|---------|
| CSV | Row | Yes (by line) | No | None/GZIP | Raw ingestion, exports |
| JSON | Row | Yes (by line) | No | None | APIs, semi-structured |
| Avro | Row | Yes | Yes (embedded) | Snappy/Deflate | Kafka, schema evolution |
| Parquet | Columnar | Yes | Yes (embedded) | Snappy/GZIP/ZSTD | Analytics, data lakes |
| ORC | Columnar | Yes | Yes | Zlib/Snappy | Hive, data warehousing |
| Delta Lake | Columnar + ACID | Yes | Yes | Snappy | Data lakehouse, ACID |

### Parquet — Deep Dive

Parquet is the default format for Spark analytics. Its columnar layout means:
- **Column pruning**: only needed columns are read from disk
- **Predicate pushdown**: row group statistics (min/max) skip irrelevant row groups
- **Excellent compression**: similar data values compress well together

```
Parquet file structure:
  Row Group 1 (e.g., 128MB)
    ├── Column chunk: customer_id
    │       ├── Page 1 (encoded + compressed)
    │       ├── Page 2
    │       └── Statistics: min=1, max=9999
    ├── Column chunk: amount
    │       └── Statistics: min=0.1, max=5000.0
    └── ...
  Row Group 2
    ...
  Footer: metadata, schema, row group offsets
```

```python
# Write Parquet with partitioning
df.write \
    .mode("overwrite") \
    .partitionBy("date", "region") \
    .option("compression", "snappy") \
    .parquet("s3://bucket/orders/")

# Read with partition pruning
df = spark.read.parquet("s3://bucket/orders/") \
    .filter(col("date") == "2024-06-01")   # only reads date=2024-06-01/ directory
```

### ORC

Optimized Row Columnar — similar to Parquet, heavily optimized for Hive. Slightly better compression than Parquet in some cases.

```python
df.write.orc("s3://bucket/table/")
df = spark.read.orc("s3://bucket/table/")
```

### Avro

Row-based format with embedded schema. Best for streaming/Kafka use cases where all columns are needed.

```python
df.write.format("avro").save("s3://bucket/events/")
```

### Delta Lake

ACID-compliant Parquet + transaction log. Adds:
- ACID transactions (concurrent reads/writes)
- Schema enforcement & evolution
- Time travel (read historical versions)
- Upserts and deletes (MERGE, UPDATE, DELETE)

```python
# Write
df.write.format("delta").mode("overwrite").save("s3://bucket/delta/orders/")

# Read
df = spark.read.format("delta").load("s3://bucket/delta/orders/")

# Time travel
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("s3://bucket/delta/orders/")

# Upsert (merge)
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
delta_table.alias("target").merge(
    updates_df.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

### Compression Codecs

| Codec | Speed | Ratio | Splittable |
|-------|-------|-------|-----------|
| None | Fastest | 1x | Yes |
| Snappy | Fast | ~2x | No (block) |
| GZIP | Slow | ~3x | No (block) |
| LZ4 | Very fast | ~1.5x | No |
| ZSTD | Fast | ~3x | No |
| Brotli | Slow | ~4x | No |

> Snappy is the default and best general choice for Parquet in Spark. ZSTD is gaining popularity for better compression without much speed penalty.

---

## 20. Performance Tuning Checklist

### Stage 1 — Reduce Data Early

```python
# Filter as early as possible
df = df.filter(col("date") >= "2024-01-01")  # before any join

# Select only needed columns before heavy operations
df = df.select("customer_id", "amount", "date")  # drop unused cols early

# Avoid SELECT * — always be explicit
```

### Stage 2 — Fix Partition Strategy

```python
# Check current partition count
print(df.rdd.getNumPartitions())

# After read: tune to match available cores
df = df.repartition(num_executors * cores_per_executor * 2)

# After shuffle: tune spark.sql.shuffle.partitions
spark.conf.set("spark.sql.shuffle.partitions", str(num_cores * 2))

# Enable AQE to handle dynamically
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Stage 3 — Optimize Joins

```python
# 1. Broadcast small tables (< 50MB)
result = large.join(broadcast(small), "key")

# 2. Filter before joining
filtered = large.filter(col("active") == True)
result = filtered.join(dim, "key")

# 3. Use bucketing for repeated large joins on same key
# (pre-sort and pre-partition at write time)

# 4. Let AQE switch join strategy at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Stage 4 — Handle Skew

```python
# Detect skew
df.groupBy("join_key").count().orderBy("count", ascending=False).show(10)

# If skewed: enable AQE skew join or use salting
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Stage 5 — Cache Strategically

```python
# Cache DataFrames used multiple times
df.cache()
df.count()  # materialize cache

# Choose right storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)  # safe default
df.persist(StorageLevel.MEMORY_ONLY)      # if data fits, faster

# Unpersist when done
df.unpersist()
```

### Stage 6 — Avoid Python UDFs

```python
# SLOW: Python UDF (serializes data to Python, executes, serializes back)
@udf("double")
def calc(x): return x * 1.18

# FAST: native Spark function (stays in JVM, Tungsten-optimized)
from pyspark.sql.functions import col
df.withColumn("result", col("price") * 1.18)

# If UDF is unavoidable: use Pandas UDF (Arrow-based)
@pandas_udf("double")
def calc_pandas(s: pd.Series) -> pd.Series:
    return s * 1.18
```

### Stage 7 — File Format & Storage

```python
# Write as Parquet (columnar, compressed, splittable)
df.write.parquet("s3://bucket/path/")

# Partition by filter columns
df.write.partitionBy("date", "region").parquet("s3://...")

# Avoid tiny files — coalesce before write
df.coalesce(10).write.parquet("s3://...")

# Use snappy compression (fast, decent ratio)
df.write.option("compression", "snappy").parquet("s3://...")
```

### Stage 8 — Memory Tuning

```python
# For memory-intensive jobs (large joins/aggregations)
--executor-memory 16g
--conf spark.memory.fraction=0.7
--conf spark.memory.storageFraction=0.4  # more execution, less storage

# For PySpark workloads
--conf spark.executor.memoryOverhead=4g

# GC tuning
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=32m"
```

### Quick Reference Config

```python
# Recommended production baseline
spark = SparkSession.builder \
    .appName("ProductionJob") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50mb") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()
```

---

## 21. Production Troubleshooting Guide

### Problem: Job is Very Slow

**Step 1: Open Spark UI → Jobs tab**
- Find the slow job → click into it

**Step 2: Identify the slow stage**
- Sort stages by duration
- Look for stages with high shuffle read/write

**Step 3: Look at tasks in the slow stage**
- Are all tasks slow? → **not skew** → partition size issue
- Is one task slow? → **data skew** → see skew section

**Step 4: Check for spill**
- Stages tab → Spill (Memory) / Spill (Disk) columns
- If > 0 → increase executor memory or increase partition count

---

### Problem: OOM — Executor Lost

**Symptom**: `ExecutorLostFailure`, `java.lang.OutOfMemoryError`

**Diagnosis checklist:**
```
1. Check stage that failed in Spark UI
2. Look at executor memory in Executors tab
3. Check if any partition is abnormally large (skew)
4. Look for spill before the OOM
```

**Fixes by cause:**

| Cause | Fix |
|-------|-----|
| Partition too large | Increase `spark.sql.shuffle.partitions` or repartition |
| Data skew | Salt the join key, or enable AQE skew join |
| Not enough executor memory | Increase `--executor-memory` |
| Large broadcast table | Lower `autoBroadcastJoinThreshold` |
| Python overhead (PySpark) | Increase `memoryOverhead` |
| Cache filling memory | Reduce `spark.memory.storageFraction`, unpersist unused caches |

---

### Problem: OOM — Driver

**Symptom**: Driver process dies, `collect()` fails

**Common causes:**
```python
# 1. Too much data collected to driver
result = df.collect()          # RISKY on large data
result = df.toPandas()         # RISKY on large data

# 2. Large broadcast table routed through driver
large_df.join(broadcast(accidentally_large_df), "key")

# 3. Large accumulators
```

**Fixes:**
```python
# Write instead of collect
df.write.parquet("s3://output/")

# Limit before collecting
df.limit(10000).collect()

# Increase driver memory
--driver-memory 8g

# Lower broadcast threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")
```

---

### Problem: Too Many Small Files (S3/HDFS)

**Symptom**: Thousands of tiny output files. Subsequent reads are slow.

**Causes:**
- Too many partitions when writing
- Streaming micro-batches writing small files

**Fix:**
```python
# Reduce partitions before write
df.coalesce(20).write.parquet("s3://...")

# Or repartition (full shuffle but even distribution)
df.repartition(20).write.parquet("s3://...")

# For Delta Lake: run OPTIMIZE periodically
# OPTIMIZE delta.`s3://bucket/path/` ZORDER BY (customer_id)
```

---

### Problem: Shuffle is Taking Too Long

**Symptoms**: Stage with large "Shuffle Write" and long duration

**Fixes:**
```python
# 1. Reduce data before shuffle (filter, project early)
df = df.filter(...).select(...)  # before groupBy/join

# 2. Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "100")  # don't use default 200 for small data

# 3. Enable AQE to coalesce small partitions
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 4. Use bucketing to eliminate shuffle entirely
# 5. Use broadcast join to eliminate shuffle

# 6. Increase shuffle buffer
spark.conf.set("spark.shuffle.file.buffer", "1mb")  # default 32kb
spark.conf.set("spark.reducer.maxSizeInFlight", "96mb")  # default 48mb
```

---

### Problem: Straggler Tasks (Data Skew)

**Symptom**: In a stage, 99 tasks finish in 2s, 1 task runs for 10+ minutes.

**Diagnosis:**
```python
# Find the hot key
df.groupBy("join_key").count().orderBy("count", ascending=False).show(10)
```

**Fix sequence:**
1. Enable AQE skew join (quickest fix, Spark 3+)
2. Salt the join key (most reliable)
3. Broadcast the smaller table (if it fits)
4. Filter out null keys separately

---

### Problem: Job Fails After Long Time

**Symptom**: Job runs for hours then fails

**Check:**
- Heartbeat timeout: `spark.executor.heartbeatInterval` (default 10s)
- Network timeout: `spark.network.timeout` (default 120s)
- If executor loses contact with driver → task fails, retried up to `spark.task.maxFailures` (default 4)

**Fix for long-running tasks:**
```python
spark.conf.set("spark.network.timeout", "800s")
spark.conf.set("spark.executor.heartbeatInterval", "60s")
spark.conf.set("spark.task.maxFailures", "8")
```

---

### Diagnostic Commands

```python
# Check config
spark.conf.get("spark.sql.shuffle.partitions")

# Check partition count
df.rdd.getNumPartitions()

# Check execution plan
df.explain()           # compact
df.explain("formatted") # readable tree
df.explain("extended") # logical + physical + codegen

# Check partition distribution
from pyspark.sql.functions import spark_partition_id
df.groupBy(spark_partition_id()).count().orderBy("count", ascending=False).show()

# Check data statistics
df.describe().show()
df.summary().show()
```

---

## 22. Interview Questions

### Spark Core

**Q: What is lazy evaluation and why does Spark use it?**
> Transformations build a logical plan without executing. Spark waits until an action is called, then uses Catalyst to optimize the full plan before execution. Benefits: predicate pushdown, column pruning, join reordering, constant folding.

**Q: What is the difference between a transformation and an action?**
> Transformations are lazy (filter, select, groupBy) — they build the DAG. Actions trigger execution (collect, show, count, write). Each action creates a Spark Job.

**Q: Explain the hierarchy: Application → Job → Stage → Task**
> Application = complete program. Job = triggered by each action. Stage = group of tasks with no shuffle boundary between them. Task = smallest unit, processes one partition. Narrow transformations stay in same stage; wide transformations (shuffle) create new stages.

**Q: What is the difference between narrow and wide transformations?**
> Narrow: output partition depends on at most one input partition — no shuffle (filter, select). Wide: output partition depends on many input partitions — requires shuffle (groupBy, join). Wide transformations create stage boundaries.

**Q: Why is shuffle expensive?**
> Shuffle writes intermediate data to disk, transfers it over the network, and reads it back on the other side. This involves disk I/O, network I/O, and sorting — all significantly slower than in-memory computation.

**Q: When would you use repartition() vs coalesce()?**
> `repartition()`: when increasing partitions or redistributing data evenly (full shuffle). `coalesce()`: when reducing partitions before writing output (no full shuffle, cheaper). `coalesce()` can produce uneven partitions.

---

### Spark Architecture

**Q: What does the Driver do vs Executors?**
> Driver: coordinates the application, builds execution plan, schedules jobs/stages/tasks, collects results. Executors: execute tasks, process data partitions, store cached data, write shuffle output.

**Q: What is the difference between Client mode and Cluster mode?**
> Client mode: driver runs on the submitting machine — stdout visible, dies if client disconnects — used for interactive work. Cluster mode: driver runs inside the cluster — survives client disconnection, logs in cluster — used for production.

**Q: What is a worker node vs an executor?**
> Worker node = machine. Executor = JVM process running on that machine. One worker node can run multiple executors.

---

### Memory Management

**Q: Explain Spark's unified memory model.**
> Spark heap is divided into: Reserved memory (Spark internals), and Unified Memory (the rest × `spark.memory.fraction`). Unified Memory is shared between Execution Memory (sort, join, aggregation) and Storage Memory (cache, persist). They can dynamically borrow from each other.

**Q: What happens when an executor runs out of memory?**
> For computation operations: Spark spills intermediate data to disk (slower but continues). For cached data: Spark evicts LRU cache partitions (recomputed from lineage on next access). If neither helps: `ExecutorLostFailure` / OOM error.

**Q: What is memory overhead and when does it matter?**
> Memory overhead is memory used outside the JVM heap — native libraries, Python workers (PySpark), off-heap memory. Critical for PySpark: Python worker processes need memory beyond `executor-memory`. Fix: increase `spark.executor.memoryOverhead`.

---

### Performance

**Q: What are the 5 join strategies in Spark and when is each used?**
> 1. **Broadcast Hash Join**: small table broadcasted to all executors — fastest, no shuffle. 2. **Sort-Merge Join**: both tables shuffled and sorted by key — scalable for large-large. 3. **Shuffle Hash Join**: one side shuffled into hash table per partition. 4. **Broadcast Nested Loop Join**: for non-equi joins — expensive O(n×m). 5. **Cartesian Product**: cross join.

**Q: What is data skew and how do you fix it?**
> Skew = some partitions are much larger than others, causing straggler tasks. Fix: (1) Enable AQE skew join — Spark 3+ auto-splits skewed partitions. (2) Salting — add random suffix to join key, explode dimension table. (3) Broadcast small table — no shuffle = no skew. (4) Filter nulls separately.

**Q: What is AQE and what problems does it solve?**
> Adaptive Query Execution (Spark 3+) re-optimizes at runtime using post-shuffle statistics. Solves: (1) too many small shuffle partitions → coalesces them. (2) data skew in joins → splits skewed partitions. (3) wrong join strategy → switches SMJ to BHJ when one side turns out small.

**Q: What is bucketing and how does it help performance?**
> Bucketing pre-sorts and pre-partitions data by a key at write time. When joining two tables bucketed on the same key with the same bucket count, Spark skips the shuffle entirely — the biggest performance win possible for repeated large joins.

**Q: How does Catalyst optimizer improve performance?**
> Catalyst applies rule-based transformations: predicate pushdown (filters close to source), column pruning (only read needed columns), partition pruning (skip irrelevant partitions), constant folding, join reordering. Then Tungsten generates optimized bytecode for execution.

---

### Production Scenarios

**Q: Your Spark job is slow. How do you debug it?**
> 1. Open Spark UI → find slow stage. 2. Check if one task is slow (skew) or all are slow (partition/resource issue). 3. Check spill columns — if > 0, increase memory or partitions. 4. Check shuffle read/write — large shuffle → consider bucketing or broadcast. 5. Check GC time in Executors tab — high GC → tune memory settings.

**Q: Your job fails with ExecutorLostFailure. What do you check?**
> 1. Executor memory usage in Spark UI. 2. Is there data skew (one partition much larger)? 3. Is there disk spill before OOM? 4. Are Python workers consuming extra memory (PySpark)? 5. Is a broadcast join table larger than estimated? Fix: increase executor-memory, increase shuffle partitions, fix skew, increase memoryOverhead.

**Q: How do you prevent small file problems?**
> Coalesce or repartition before writing. For streaming: use Delta Lake OPTIMIZE command. Avoid writing too many partitions to storage. For Parquet: target 128MB–512MB files.

**Q: When would you use cache() and when would you not?**
> Use: DataFrame accessed in multiple jobs, expensive computation result, iterative algorithms. Don't use: single-use DataFrame, very large data that won't fit in memory, fast-source data (Parquet already optimized). Always unpersist when done.

---

*Last updated from: https://github.com/rahulopt/spark-internals-performance*
*Enhanced with: AQE, Data Skew/Salting, GC Tuning, Spark Submit, Serialization, Tungsten, Predicate Pushdown, File Formats, Production Troubleshooting*
