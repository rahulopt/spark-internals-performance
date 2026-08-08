# 16. Repartition — Real-World Examples

In the previous lesson, we learned what `repartition()` does internally.

Now let's move from theory to **real-world Spark scenarios**.

The goal is not to memorize:

```python
df.repartition(100)
```

The goal is to understand:

> **When should I repartition, why should I repartition, and how do I know whether it actually helped?**

---

# 📌 Scenario 1 — Too Few Partitions

Suppose we have:

```text
500 GB Data
```

but the DataFrame has only:

```text
10 partitions
```

Conceptually:

```text
500 GB
   ↓
10 partitions
   ↓
~50 GB per partition
```

This can create very large tasks.

```text
Partition 0 → ~50 GB
Partition 1 → ~50 GB
Partition 2 → ~50 GB
...
```

If the cluster has many executor cores available, only 10 tasks can run for that stage.

```text
Available cores → 100
Partitions      → 10
```

Only around:

```text
10 tasks
```

can be active for that stage.

A lot of cluster capacity may remain unused.

---

## 🔹 Possible Solution

Increase the number of partitions:

```python
df = df.repartition(200)
```

Now:

```text
500 GB
   ↓
200 partitions
   ↓
~2.5 GB average
```

Conceptually:

```text
Before:

10 partitions
     ↓
Large tasks
     ↓
Low parallelism


After:

200 partitions
     ↓
Smaller tasks
     ↓
More parallelism
```

### But Remember

The partition sizes will not necessarily be exactly equal.

Data distribution matters.

---

# 📌 Scenario 2 — Too Many Partitions

Now consider the opposite.

Suppose:

```text
1 GB Data
```

and:

```text
100,000 partitions
```

You could end up with extremely small partitions.

```text
1 GB
 ↓
100,000 partitions
 ↓
Tiny chunks
```

This can result in:

```text
100,000 tasks
```

for a stage.

Spark now spends significant time on:

* Task scheduling
* Task startup
* Serialization
* Coordination

instead of processing useful data.

---

## 🔹 Possible Solution

Reduce the number of partitions.

If you only need fewer partitions:

```python
df = df.coalesce(100)
```

or, when redistribution is specifically required:

```python
df = df.repartition(100)
```

The choice depends on whether you need a full redistribution.

---

# 📌 Scenario 3 — Controlling Output File Count

This is one of the most common practical uses.

Suppose:

```python
df.write.parquet(
    "s3a://bucket/orders/"
)
```

and the DataFrame has:

```text
5000 partitions
```

You may end up with a large number of output files.

Conceptually:

```text
5000 partitions
      ↓
Write
      ↓
Many output files
```

This can create a **small files problem**.

---

## 🔹 Why Small Files Are Bad

Suppose S3 contains:

```text
orders/
├── part-00001.parquet
├── part-00002.parquet
├── part-00003.parquet
├── ...
└── part-50000.parquet
```

Now downstream jobs may have to manage thousands of files.

Problems can include:

* File listing overhead
* More metadata operations
* More tasks
* More scheduling overhead
* Poor query performance in some workloads

---

## 🔹 Possible Solution

Reduce partitions before writing:

```python
df = df.coalesce(100)

df.write.parquet(
    "s3a://bucket/orders/"
)
```

Or:

```python
df = df.repartition(100)

df.write.parquet(
    "s3a://bucket/orders/"
)
```

The right choice depends on whether you need redistribution.

---

# 📌 Scenario 4 — Repartition Before Writing to S3

Imagine:

```text
Input
 ↓
5000 partitions
 ↓
Transformations
 ↓
5000 output tasks
 ↓
5000 files
```

Instead:

```python
df = df.repartition(200)

df.write.parquet(
    "s3a://bucket/orders/"
)
```

Conceptually:

```text
5000 partitions
       ↓
Repartition
       ↓
200 partitions
       ↓
Write
       ↓
~200 output files
```

This can be useful when the output would otherwise create an excessive number of files.

---

# ⚠️ But There Is a Cost

Remember:

```python
df.repartition(200)
```

causes a shuffle.

So:

```text
5000 partitions
      ↓
Shuffle
      ↓
200 partitions
      ↓
Write
```

You are trading:

```text
Extra shuffle cost
```

for:

```text
Better output file layout
```

Whether this is worth it depends on the workload.

---

# 📌 Scenario 5 — Preparing Data for a Join

Suppose we have:

```text
Orders
Customers
```

and the join key is:

```text
customer_id
```

You might think:

```python
orders_df = orders_df.repartition(
    "customer_id"
)

customers_df = customers_df.repartition(
    "customer_id"
)
```

then:

```python
orders_df.join(
    customers_df,
    "customer_id"
)
```

---

## 🔹 Important Warning

Don't automatically do this.

Why?

Because the join itself may already require Spark to redistribute data.

Conceptually:

```text
Join
 ↓
Exchange
 ↓
Shuffle
```

If you manually repartition first:

```text
Manual repartition
 ↓
Shuffle
 ↓
Join
 ↓
Possible additional exchange
 ↓
Shuffle
```

You could create unnecessary work.

Therefore:

> **Don't repartition just because a join exists.**

First inspect the physical plan.

```python
joined_df.explain("formatted")
```

---

# 📌 Scenario 6 — Reusing the Same Partitioning

There are situations where explicit repartitioning can make sense.

Suppose you repeatedly perform operations around the same key:

```text
customer_id
```

For example:

```python
df = df.repartition(
    "customer_id"
).cache()
```

Then several downstream operations use the cached DataFrame.

```text
Original Data
     ↓
Repartition by customer_id
     ↓
Cache
     ↓
+---------+---------+
|         |         |
↓         ↓         ↓
GroupBy   Join     Aggregation
```

Here, paying the shuffle cost once may make sense if the repartitioned result is reused enough.

---

# 📌 Scenario 7 — Uneven Partition Sizes

Suppose your DataFrame has:

```text
10 partitions
```

but the actual sizes look like:

```text
Partition 0 → 1 GB
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 1 GB
Partition 5 → 1 GB
Partition 6 → 1 GB
Partition 7 → 1 GB
Partition 8 → 1 GB
Partition 9 → 50 GB
```

The total partition count looks fine.

But the distribution is terrible.

This can cause:

```text
Partition 9
    ↓
Huge Task
    ↓
Slow Task
    ↓
Stage waits
```

This is a form of **data skew / uneven distribution**.

---

# 🔹 The Straggler Problem

Suppose task execution times are:

```text
Task 1 → 20 sec
Task 2 → 22 sec
Task 3 → 19 sec
Task 4 → 21 sec
Task 5 → 20 sec
Task 6 → 95 sec
```

Most tasks finish quickly.

But one task takes much longer.

The stage cannot finish until the slow task completes.

```text
Fast Tasks
   ↓
Finished

Slow Task
   ↓
Still Running
   ↓
Stage waits
```

This is called a **straggler task**.

---

# 📌 Scenario 8 — Repartition by a Skewed Column

Suppose:

```python
df.repartition(
    "customer_id"
)
```

But one customer is extremely common:

```text
customer_id = 999
```

with:

```text
500 million records
```

The hash partitioning may send those records to the same target partition.

Conceptually:

```text
customer_id = 999
        ↓
Hash
        ↓
Partition 7
        ↓
Huge amount of data
```

Result:

```text
Partition 0 → 2 GB
Partition 1 → 2 GB
Partition 2 → 2 GB
Partition 3 → 2 GB
Partition 4 → 2 GB
Partition 5 → 2 GB
Partition 6 → 2 GB
Partition 7 → 100 GB
```

Simply calling:

```python
repartition("customer_id")
```

did not solve the skew.

It exposed the skew more clearly.

---

# 📌 Scenario 9 — Salting a Skewed Key

One technique for handling extreme skew is **salting**.

Suppose:

```text
customer_id = 999
```

has an enormous number of records.

Instead of using only:

```text
999
```

we can create a salted key:

```text
999_0
999_1
999_2
999_3
...
```

Conceptually:

```text
999
 ↓
Salt
 ↓
999_0
999_1
999_2
999_3
```

Now the records can be distributed across multiple partitions.

Example:

```python
from pyspark.sql import functions as F

df = df.withColumn(
    "salt",
    F.floor(F.rand() * 10)
)

df = df.withColumn(
    "salted_customer_id",
    F.concat_ws(
        "_",
        F.col("customer_id"),
        F.col("salt")
    )
)
```

This is only a conceptual example.

Salting joins requires applying compatible salting logic to both sides.

---

# 📌 Scenario 10 — Repartition After Filtering

Suppose we start with:

```text
1 TB
```

and filter:

```python
filtered_df = df.filter(
    "country = 'India'"
)
```

Now only:

```text
50 GB
```

remain.

The original partition structure may no longer be ideal.

You could consider:

```python
filtered_df = filtered_df.repartition(
    100
)
```

But again:

> **Only do this when there is a real downstream reason.**

For example:

```text
Filter
 ↓
50 GB
 ↓
Repartition
 ↓
100 partitions
 ↓
Expensive Join / Aggregation
```

If the next operation benefits from the new distribution, the shuffle may be worth it.

---

# 📌 Scenario 11 — Repartition Before Expensive Aggregation

Suppose:

```python
df.groupBy(
    "customer_id"
).agg(
    F.sum("amount")
)
```

The aggregation itself may require a shuffle.

If you manually do:

```python
df.repartition(
    "customer_id"
)
```

before it, you're potentially adding another shuffle.

So the default approach should be:

```python
result = df.groupBy(
    "customer_id"
).agg(
    F.sum("amount")
)
```

Then inspect:

```python
result.explain("formatted")
```

Don't add repartitioning unless profiling shows a reason.

---

# 📌 Scenario 12 — Repartition and Number of Executors

Suppose your cluster has:

```text
10 Executors
4 cores per executor
```

Total available cores:

```text
10 × 4 = 40 cores
```

If your stage has:

```text
4 partitions
```

then only around:

```text
4 tasks
```

can execute concurrently for that stage.

You may be under-utilizing the cluster.

If you have:

```text
100 partitions
```

you can have enough tasks to keep the available cores busy.

```text
100 partitions
       ↓
40 tasks can run
       ↓
Remaining tasks wait
       ↓
Next wave runs
```

This is why partition count affects parallelism.

---

# 📌 Scenario 13 — Partition Count Is Not the Same as CPU Cores

A common misconception:

> "I have 40 cores, so I should create exactly 40 partitions."

Not necessarily.

Suppose:

```text
40 cores
```

and:

```text
40 partitions
```

Then there may be only one wave of tasks.

If some tasks are slow, the cluster may not have enough flexibility.

Having more partitions can allow Spark to schedule work in multiple waves.

For example:

```text
40 cores
+
200 partitions
```

means:

```text
Wave 1 → 40 tasks
Wave 2 → 40 tasks
Wave 3 → 40 tasks
Wave 4 → 40 tasks
Wave 5 → 40 tasks
```

The actual scheduling depends on the application and cluster.

---

# 📌 Scenario 14 — Partition Count and Task Size

A useful way to think about partitioning is:

```text
Data Size
   ÷
Number of Partitions
   ≈
Average Partition Size
```

For example:

```text
100 GB
÷
100 partitions
=
~1 GB per partition
```

This is only an approximation.

Actual sizes can vary because of:

* Compression
* File format
* Data distribution
* Partitioning strategy
* Skew

---

# 📌 Scenario 15 — Why "128 MB per Partition" Is Not a Universal Formula

You may hear:

```text
Data Size / 128 MB
```

as a partition calculation.

This is a useful **rule of thumb in some contexts**, but it is not a universal Spark formula.

Spark does not simply say:

```text
3072 MB
÷
128 MB
=
24 partitions
```

and always create exactly 24 partitions.

The actual partition count can depend on:

* Input file sizes
* File splitting
* Input format
* `spark.sql.files.maxPartitionBytes`
* Existing partitioning
* Shuffle configuration
* Number of shuffle partitions
* Data source
* Execution plan

Therefore:

> **Never memorize `data size / 128 MB` as "Spark's partition formula."**

---

# 📌 Scenario 16 — Input Partitions vs Shuffle Partitions

This distinction is extremely important.

There are different points where Spark creates partitions.

### Reading Data

When Spark reads files:

```text
S3 / HDFS / Files
       ↓
Input Partitions
```

The number depends on the input files and file-splitting configuration.

---

### Shuffle

After a wide transformation:

```text
groupBy()
join()
repartition()
       ↓
Shuffle
       ↓
Shuffle Partitions
```

The default number of shuffle partitions is controlled by:

```python
spark.conf.get(
    "spark.sql.shuffle.partitions"
)
```

So:

```text
Input partitions
```

and:

```text
Shuffle partitions
```

are not necessarily the same thing.

---

# 📌 Scenario 17 — Changing Shuffle Partitions

Suppose:

```python
spark.conf.get(
    "spark.sql.shuffle.partitions"
)
```

returns:

```text
200
```

Then a shuffle operation may create:

```text
200 shuffle partitions
```

For example:

```python
df.groupBy(
    "customer_id"
).count()
```

may use the configured shuffle partition count.

You can change it:

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    500
)
```

Now future shuffle operations can use:

```text
500 partitions
```

subject to the execution plan and adaptive optimizations.

---

# 📌 Scenario 18 — `repartition()` vs `spark.sql.shuffle.partitions`

These are related but different.

### Explicit repartition

```python
df.repartition(100)
```

You are explicitly requesting:

```text
100 target partitions
```

### Shuffle configuration

```python
spark.conf.set(
    "spark.sql.shuffle.partitions",
    500
)
```

This config influences the default number of partitions used for certain shuffle operations.

Think:

```text
repartition(100)
      ↓
Explicit instruction
```

while:

```text
spark.sql.shuffle.partitions = 500
      ↓
Default shuffle configuration
```

---

# 📌 Scenario 19 — AQE Changes the Picture

Modern Spark can use **Adaptive Query Execution (AQE)**.

Check:

```python
spark.conf.get(
    "spark.sql.adaptive.enabled"
)
```

AQE allows Spark to make certain decisions using runtime statistics.

For example:

```text
Initial Plan
     ↓
Execute
     ↓
Observe Runtime Statistics
     ↓
Optimize
     ↓
Continue Execution
```

AQE can help with:

* Coalescing small shuffle partitions
* Skewed joins
* Dynamic changes to certain join strategies

Therefore, manually tuning partition counts without checking AQE can sometimes be unnecessary.

---

# 📌 Scenario 20 — Real Production Example

Imagine an ETL pipeline:

```text
S3
 ↓
5 TB Parquet
 ↓
Filter
 ↓
Join Customers
 ↓
GroupBy Region
 ↓
Write to S3
```

Suppose the initial job is slow.

Don't immediately write:

```python
df.repartition(1000)
```

Instead, investigate.

---

## Step 1 — Check Input

```python
df.rdd.getNumPartitions()
```

---

## Step 2 — Inspect Plan

```python
df.explain("formatted")
```

Look for:

```text
Exchange
SortMergeJoin
BroadcastHashJoin
```

---

## Step 3 — Execute

Trigger the job:

```python
result_df.count()
```

---

## Step 4 — Open Spark UI

Check:

```text
Stage
 ↓
Tasks
 ↓
Shuffle Read
Shuffle Write
```

---

## Step 5 — Find the Bottleneck

Possible findings:

```text
Finding 1:
Only 20 partitions
→ Too little parallelism


Finding 2:
100,000 tiny partitions
→ Too much scheduling overhead


Finding 3:
One task much slower
→ Possible skew


Finding 4:
Huge shuffle
→ Expensive data redistribution


Finding 5:
Thousands of output files
→ Small files problem
```

---

## Step 6 — Apply the Correct Solution

```text
Too few partitions
      ↓
repartition()

Too many output partitions
      ↓
coalesce()

Small table
      ↓
broadcast()

Data skew
      ↓
AQE / salting / skew handling

Huge unnecessary data
      ↓
Filter / select earlier
```

---

# 🔹 Real-World Rule

Before adding:

```python
repartition()
```

ask:

```text
1. Why is the current partitioning bad?

2. What problem am I solving?

3. Will redistribution improve the next operation?

4. How much shuffle will this introduce?

5. Can Spark already handle this automatically?

6. Did Spark UI show a real bottleneck?
```

If you cannot answer these questions, don't repartition blindly.

---

# 🧠 Key Takeaways

* `repartition()` is a **performance tool**, not a default requirement.
* Use it when you have a clear reason to redistribute data.
* Too few partitions can cause poor parallelism.
* Too many partitions can cause scheduling overhead.
* Repartitioning before writing can help control excessive output files.
* Repartitioning by a key can help organize data for downstream operations.
* But repartitioning before every join or aggregation can create unnecessary shuffles.
* `repartition()` itself causes a shuffle.
* Data skew can still exist after repartitioning.
* A single extremely common key can create a huge partition.
* Skew can produce **straggler tasks**.
* Cluster cores and partition count are related, but they do not need to be equal.
* More partitions can allow multiple waves of tasks.
* Input partitions and shuffle partitions are different concepts.
* `spark.sql.shuffle.partitions` controls the default number of shuffle partitions for relevant operations.
* `repartition(n)` explicitly requests `n` target partitions.
* AQE can dynamically optimize partitioning and execution decisions.
* Always use **Spark UI + `explain()`** when tuning a real job.
* Performance tuning should be based on **measurement**, not guesses.

---

# ⭐ Most Important Mental Model

Don't think:

```text
Big Data
   ↓
More partitions
   ↓
Faster
```

Think:

```text
                 Data
                   ↓
             Current Layout
                   ↓
            Is it a problem?
                   ↓
          +--------+--------+
          |                 |
         No                Yes
          |                 |
          ↓                 ↓
      Don't touch      Find bottleneck
                            |
                +-----------+-----------+
                |           |           |
                ↓           ↓           ↓
             Too Few     Too Many     Skew
                |           |           |
                ↓           ↓           ↓
          repartition    coalesce    AQE/
                                     salting
```

---

# 🚀 Final Real-World Picture

```text
                     Spark Job
                         |
                         ↓
                     Read Data
                         |
                         ↓
                  Input Partitions
                         |
                         ↓
                   Transformations
                         |
                         ↓
                 Is Shuffle Needed?
                         |
                 +-------+-------+
                 |               |
                Yes              No
                 |               |
                 ↓               ↓
              Shuffle        Continue
                 |
                 ↓
          Shuffle Partitions
                 |
                 ↓
             Tasks Execute
                 |
                 ↓
             Spark UI
                 |
        +--------+--------+
        |        |        |
        ↓        ↓        ↓
      Size     Skew    Shuffle
        |        |        |
        +--------+--------+
                 |
                 ↓
          Tune if necessary
                 |
                 ↓
       repartition / coalesce /
       broadcast / AQE / filter
```

> **The best partition count is not a magic number. It is the partitioning that gives your workload good parallelism, reasonable task sizes, balanced data, and acceptable shuffle overhead.**
