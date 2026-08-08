# 13. Join Operations — Part 2

In Part 1, we learned the basics of joins and why joins are different in a distributed system.

In this part, we will go deeper into **how Spark actually executes joins** and how to identify expensive joins.

---

# 📌 Join Execution Flow

When we write:

```python
joined_df = orders_df.join(
    customers_df,
    "customer_id"
)
```

Spark does not immediately execute the join.

The execution goes through several stages:

```text
DataFrame API
     ↓
Logical Plan
     ↓
Catalyst Optimizer
     ↓
Physical Plan
     ↓
Join Strategy
     ↓
Execution
```

Spark may choose different physical strategies depending on the data and query.

The major strategies are:

```text
1. Broadcast Hash Join
2. Sort Merge Join
3. Shuffled Hash Join
4. Broadcast Nested Loop Join
```

---

# 🔹 1. Broadcast Hash Join

A **Broadcast Hash Join** is useful when one side of the join is small enough to be broadcast to all executors.

Suppose:

```text
Orders      → 500 GB
Customers   → 20 MB
```

Instead of shuffling the 500 GB orders DataFrame, Spark can broadcast the 20 MB customers DataFrame.

```text
                    Customers
                       20 MB
                         |
                    Broadcast
                  /     |     \
                 ↓      ↓      ↓
            Executor  Executor  Executor
                1        2        3
                |        |        |
             Orders    Orders    Orders
                |        |        |
                +--------+--------+
                         |
                        JOIN
```

### Main Idea

```text
Small DataFrame
       ↓
   Broadcast
       ↓
  All Executors
       ↓
Local Join
```

The large DataFrame does not need to be shuffled just to make the join possible.

---

## 🔹 Broadcast Join Example

```python
from pyspark.sql.functions import broadcast

joined_df = orders_df.join(
    broadcast(customers_df),
    "customer_id",
    "inner"
)
```

Here we are explicitly telling Spark:

```text
customers_df
     ↓
broadcast this DataFrame
```

---

# 🔹 Automatic Broadcast

Spark can also automatically choose a broadcast join when the smaller side is below the configured broadcast threshold and the plan allows it.

Check the current setting:

```python
spark.conf.get(
    "spark.sql.autoBroadcastJoinThreshold"
)
```

The threshold is configurable.

For example:

```python
spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    50 * 1024 * 1024
)
```

This sets the threshold to approximately:

```text
50 MB
```

> The optimizer still decides whether the broadcast strategy is appropriate.

---

# 🔹 Why Broadcast Join Is Fast

Without broadcasting:

```text
Large Data
    ↓
Shuffle
    ↓
Network Transfer
    ↓
Join
```

With broadcasting:

```text
Small Data
    ↓
Broadcast
    ↓
Executors
    ↓
Local Join
```

The major advantage is:

> **Avoiding a large shuffle of the big DataFrame.**

---

# ⚠️ Broadcast Join Limitation

Broadcasting is not always good.

Suppose:

```text
Orders      → 500 GB
Customers   → 200 GB
```

Broadcasting 200 GB to every executor can consume a huge amount of memory and create significant network overhead.

Therefore:

```text
Small Table
    ↓
Broadcast
    ↓
Good
```

but:

```text
Huge Table
    ↓
Broadcast
    ↓
Dangerous / Expensive
```

Never blindly broadcast a large DataFrame.

---

# 🔹 2. Sort Merge Join

When both datasets are large, Spark can use a **Sort Merge Join**.

Example:

```text
Orders      → 500 GB
Customers   → 200 GB
```

Broadcasting `customers` is not practical.

Instead, Spark can redistribute both datasets based on the join key.

```text
Orders
   ↓
Shuffle by customer_id
   ↓
Sort
   ↓
+
   |
   | Merge
   |
   +
   ↓
Join
```

The same happens for the other side:

```text
Customers
   ↓
Shuffle by customer_id
   ↓
Sort
```

Conceptually:

```text
Orders                       Customers
   |                             |
   ↓                             ↓
Shuffle                       Shuffle
   |                             |
   ↓                             ↓
Sort                          Sort
   |                             |
   +-------------+---------------+
                 |
                Merge
                 |
                 ↓
                JOIN
```

---

# 🔹 Why Is It Called Sort Merge Join?

Because two important operations happen:

```text
SORT
  +
MERGE
```

First, data is sorted according to the join key.

Then Spark merges the sorted datasets.

For example:

```text
Orders

101
102
103
105
108
```

and:

```text
Customers

101
103
105
110
```

Spark can efficiently compare them:

```text
101 → Match
102 → No Match
103 → Match
105 → Match
108 → No Match
110 → No Match
```

---

# 🔹 Shuffle in Sort Merge Join

If the data is not already appropriately partitioned, Spark needs to redistribute it.

```text
Orders
   ↓
Shuffle by customer_id
   ↓
Partitions

Customers
   ↓
Shuffle by customer_id
   ↓
Partitions
```

Therefore:

```text
Sort Merge Join
      ↓
Possible Shuffle
      ↓
Sort
      ↓
Merge
      ↓
Join
```

This is why large joins can become expensive.

---

# 🔹 3. Shuffled Hash Join

Another strategy is **Shuffled Hash Join**.

Conceptually:

```text
Left DataFrame
      ↓
    Shuffle
      ↓
Hash Partition

Right DataFrame
      ↓
    Shuffle
      ↓
Hash Partition

      ↓
Hash Join
```

The key difference is:

```text
Sort Merge Join

Shuffle
   ↓
Sort
   ↓
Merge
```

versus:

```text
Shuffled Hash Join

Shuffle
   ↓
Hash Join
```

Shuffled Hash Join can be useful when one side is small enough to build a hash table per partition.

---

# 🔹 4. Broadcast Nested Loop Join

A **Broadcast Nested Loop Join** is different from the normal hash-based equi-joins.

Conceptually, Spark can compare rows from one side against rows from the broadcasted side.

```text
Large Data
    |
    +---- Row A
    |      ↓
    |   Compare with
    |   broadcast rows
    |
    +---- Row B
           ↓
        Compare with
        broadcast rows
```

This can be useful for certain join conditions that cannot be handled efficiently by normal equality-based joins.

For example:

```python
orders_df.join(
    customers_df,
    orders_df.amount > customers_df.minimum_amount
)
```

---

# 🔹 Equi Join

An **equi join** uses equality between join keys.

Example:

```python
orders_df.customer_id == customers_df.customer_id
```

or:

```python
orders_df.join(
    customers_df,
    "customer_id"
)
```

This is the most common type of join in data engineering.

Spark has highly optimized physical strategies for equi-joins.

---

# 🔹 Non-Equi Join

A non-equi join uses conditions other than simple equality.

Examples:

```text
>
<
>=
<=
!=
```

Example:

```python
orders_df.join(
    customers_df,
    orders_df.amount > customers_df.minimum_amount
)
```

These joins may require different physical strategies.

---

# 🔹 How Does Spark Choose the Strategy?

This is one of the most important questions.

When we write:

```python
df1.join(df2, "id")
```

we usually don't specify:

```text
"Use Broadcast Hash Join"
```

Spark's optimizer decides.

Conceptually:

```text
Logical Join
      ↓
Catalyst Optimizer
      ↓
Statistics
      ↓
Configuration
      ↓
Available Strategies
      ↓
Physical Plan
```

Spark can consider things such as:

* Size of each relation
* Table statistics
* Join type
* Join condition
* Broadcast threshold
* Available memory
* Spark SQL configurations

---

# 🔹 `explain()` — See What Spark Is Actually Doing

One of the most useful Spark debugging techniques is:

```python
joined_df.explain()
```

For a more readable plan:

```python
joined_df.explain("formatted")
```

This allows us to inspect the physical execution plan.

---

# 🔹 Example

```python
joined_df = orders_df.join(
    customers_df,
    "customer_id",
    "inner"
)

joined_df.explain("formatted")
```

You may see:

```text
BroadcastHashJoin
```

or:

```text
SortMergeJoin
```

This tells us which physical join strategy Spark selected.

---

# 🔹 Reading `BroadcastHashJoin`

If the plan contains:

```text
BroadcastHashJoin
```

it means Spark decided to broadcast one side.

Conceptually:

```text
Small Table
     ↓
Broadcast
     ↓
Executors
     ↓
Hash Join
```

This is generally good when one side is genuinely small.

---

# 🔹 Reading `SortMergeJoin`

If the plan contains:

```text
SortMergeJoin
```

Spark is using the sort-merge strategy.

You may see something similar to:

```text
SortMergeJoin
   |
   +--- Sort
   |      |
   |   Exchange
   |
   +--- Sort
          |
       Exchange
```

The `Exchange` operators are important.

---

# 🔹 What Is `Exchange`?

`Exchange` generally represents a redistribution of data between partitions.

For example:

```text
Exchange
   ↓
Sort
   ↓
SortMergeJoin
```

This often means Spark is performing a shuffle before the join.

Think:

```text
Exchange
    ≈
Data redistribution between partitions
```

---

# 🔹 Why Is `Exchange` Important?

Suppose we have:

```text
Partition 0 → customer 101
Partition 1 → customer 102
Partition 2 → customer 103
```

But Spark needs records with the same key to be in the same partition.

It may perform:

```text
Current Partitions
       ↓
    Exchange
       ↓
New Partitions
```

After redistribution:

```text
Partition 0 → customer 101, 101
Partition 1 → customer 102, 102
Partition 2 → customer 103, 103
```

Now the join can efficiently match the records.

---

# 🔹 Exchange = Potential Shuffle

A very useful mental model:

```text
Exchange
   ↓
Think:
"Spark is redistributing data."
```

In a physical plan, you may see:

```text
Exchange
   ↓
Sort
   ↓
SortMergeJoin
```

This is a strong indication that the join involves shuffle/exchange.

---

# 🔹 `explain()` Is Better Than Guessing

Don't say:

> "This join probably causes a shuffle."

Check it.

```python
joined_df.explain("formatted")
```

Then inspect:

```text
Exchange
SortMergeJoin
BroadcastHashJoin
```

This gives you evidence about the physical plan.

---

# 🔹 Spark UI and Joins

`explain()` tells us **what Spark plans to do**.

Spark UI helps us understand **what actually happened during execution**.

After triggering an action:

```python
joined_df.count()
```

open Spark UI.

Look at:

```text
SQL
  ↓
Stages
  ↓
Tasks
```

---

# 🔹 What to Look for in Spark UI

For an expensive join, pay attention to:

### 1. Shuffle Read

How much data a stage reads from shuffle.

### 2. Shuffle Write

How much data the stage writes for downstream stages.

### 3. Stage Duration

How long the stage takes.

### 4. Task Duration

Look for tasks that take significantly longer than others.

### 5. Input Size

How much data the stage processes.

---

# 🔹 Example of a Shuffle-Heavy Join

Imagine:

```text
Stage 1
   ↓
Shuffle Write = 300 GB
   ↓
Stage 2
   ↓
Shuffle Read = 300 GB
   ↓
SortMergeJoin
```

This can be expensive.

Why?

Because Spark has to:

```text
Read data
   ↓
Partition data
   ↓
Serialize
   ↓
Transfer over network
   ↓
Write shuffle data
   ↓
Read shuffle data
   ↓
Sort
   ↓
Join
```

---

# 🔹 Join Performance Problem #1 — Large Shuffle

Suppose:

```text
Orders      = 1 TB
Customers   = 500 GB
```

A large shuffle may happen.

```text
1 TB
 ↓
Shuffle
 ↓
Network
 ↓
Sort
 ↓
Join
```

This can take significant time.

### Possible Solution

If one side is actually small enough:

```text
Broadcast Join
```

But if both sides are huge, we need other optimizations.

---

# 🔹 Join Performance Problem #2 — Data Skew

This is one of the most important Spark join problems.

Suppose we join using:

```text
customer_id
```

Normally:

```text
customer 101 → 100 rows
customer 102 → 120 rows
customer 103 → 90 rows
```

But one customer has:

```text
customer 999 → 500 million rows
```

This is **data skew**.

---

# 🔹 What Happens With Data Skew?

Spark distributes data across partitions.

Ideally:

```text
Partition 0 → 100 GB
Partition 1 → 100 GB
Partition 2 → 100 GB
Partition 3 → 100 GB
```

But with skew:

```text
Partition 0 → 100 GB
Partition 1 → 105 GB
Partition 2 → 95 GB
Partition 3 → 500 GB
```

Now:

```text
Task 1 → 10 minutes
Task 2 → 11 minutes
Task 3 → 9 minutes
Task 4 → 60 minutes
```

The entire stage may wait for the slow task.

This is called a **straggler task**.

---

# 🔹 Data Skew Mental Model

```text
Normal

Data
 ↓
Even Distribution
 ↓
Balanced Partitions
 ↓
Balanced Tasks
```

With skew:

```text
Data
 ↓
Uneven Distribution
 ↓
One Huge Partition
 ↓
One Slow Task
 ↓
Stage Takes Longer
```

---

# 🔹 How Can We Handle Data Skew?

Some common approaches include:

* AQE skew join optimization
* Salting
* Better partitioning
* Filtering unnecessary data before the join
* Broadcasting a genuinely small side
* Choosing a better join strategy

One important Spark feature is:

```text
Adaptive Query Execution (AQE)
```

AQE can dynamically optimize query execution based on runtime statistics.

---

# 🔹 Adaptive Query Execution

AQE can be enabled with:

```python
spark.conf.get(
    "spark.sql.adaptive.enabled"
)
```

Typically:

```text
AQE
 ↓
Query starts
 ↓
Spark observes runtime statistics
 ↓
Plan can be adjusted
```

AQE can help with issues such as:

* Coalescing small shuffle partitions
* Handling skewed joins
* Dynamically changing certain execution decisions

---

# 🔹 Join Performance Problem #3 — Too Many Small Files

Suppose the DataFrame is built from thousands of tiny files:

```text
file1.parquet
file2.parquet
file3.parquet
...
file100000.parquet
```

This can create many tasks and additional overhead before the join even starts.

A better storage layout can improve the overall workload.

This is why file sizing and partitioning are also important for join performance.

---

# 🔹 Join Performance Problem #4 — Joining Unnecessary Data

Avoid carrying unnecessary columns into a large join.

Instead of:

```python
orders_df.join(
    customers_df,
    "customer_id"
)
```

consider selecting only required columns:

```python
customers_small = customers_df.select(
    "customer_id",
    "name",
    "region"
)

joined_df = orders_df.join(
    customers_small,
    "customer_id"
)
```

Benefits:

```text
Less data
   ↓
Less memory
   ↓
Less network transfer
   ↓
Less shuffle
   ↓
Better performance
```

---

# 🔹 Filter Before Join

If possible, filter data before joining.

Instead of:

```python
joined_df = orders_df.join(
    customers_df,
    "customer_id"
).filter(
    "region = 'North'"
)
```

we may be able to reduce the data earlier:

```python
customers_filtered = customers_df.filter(
    "region = 'North'"
)

joined_df = orders_df.join(
    customers_filtered,
    "customer_id"
)
```

Now the join may process less data.

Spark's optimizer can sometimes push filters down automatically, but writing logically efficient queries is still important.

---

# 🔹 Repartitioning Before Join

You can explicitly repartition a DataFrame:

```python
orders_df = orders_df.repartition(
    "customer_id"
)
```

and:

```python
customers_df = customers_df.repartition(
    "customer_id"
)
```

Conceptually:

```text
Orders
   ↓
Partition by customer_id

Customers
   ↓
Partition by customer_id

        ↓
      JOIN
```

However, `repartition()` itself causes a shuffle.

Therefore, don't add repartitioning blindly.

Always understand why you are doing it.

---

# 🔹 `coalesce()` vs `repartition()`

These are different operations.

### `repartition()`

```python
df.repartition("customer_id")
```

Can cause a shuffle.

It is useful when you need to redistribute data.

### `coalesce()`

```python
df.coalesce(10)
```

Typically reduces the number of partitions without a full shuffle.

It is mainly useful when reducing partitions.

```text
repartition()
    ↓
Redistribute data
    ↓
Shuffle possible
```

```text
coalesce()
    ↓
Reduce partitions
    ↓
Usually avoids full shuffle
```

---

# 🔹 Broadcast vs Repartition

Suppose:

```text
Orders      = 500 GB
Customers   = 20 MB
```

A reasonable approach may be:

```text
Broadcast Customers
```

instead of:

```text
Repartition both DataFrames
```

But if:

```text
Orders      = 500 GB
Customers   = 300 GB
```

broadcasting is not practical.

A shuffle-based strategy may be more appropriate.

---

# 🔹 Join Strategy Summary

| Strategy              | Best Situation                   | Main Cost                         |
| --------------------- | -------------------------------- | --------------------------------- |
| Broadcast Hash Join   | Small + Large                    | Broadcast memory/network          |
| Sort Merge Join       | Large + Large                    | Shuffle + Sort                    |
| Shuffled Hash Join    | Suitable hash-join scenario      | Shuffle + Hash                    |
| Broadcast Nested Loop | Small side + non-equi conditions | Potentially expensive comparisons |

---

# 🔹 Complete Physical Execution Picture

```text
                    JOIN
                      |
                      ↓
                Logical Plan
                      |
                      ↓
             Catalyst Optimizer
                      |
                      ↓
                Physical Plan
                      |
          +-----------+-----------+
          |                       |
          ↓                       ↓
   Broadcast Hash         Sort Merge Join
       Join                     |
          |                     |
          ↓                     ↓
     Broadcast              Exchange
          |                     |
          ↓                     ↓
    Local Hash Join          Sort
                                |
                                ↓
                              Merge
                                |
                                ↓
                               JOIN
```

---

# 🔹 Practical Debugging Workflow

When a Spark join is slow, follow this process.

### Step 1 — Look at the Data Size

```python
df.printSchema()
```

Understand what you're joining.

---

### Step 2 — Check the Join

```python
joined_df = df1.join(
    df2,
    "customer_id"
)
```

Understand the join key.

---

### Step 3 — Inspect the Physical Plan

```python
joined_df.explain("formatted")
```

Look for:

```text
BroadcastHashJoin
SortMergeJoin
Exchange
Sort
```

---

### Step 4 — Trigger the Query

```python
joined_df.count()
```

or another action.

---

### Step 5 — Open Spark UI

Check:

```text
SQL
 ↓
Stages
 ↓
Tasks
```

Look at:

```text
Shuffle Read
Shuffle Write
Stage Duration
Task Duration
Input Size
```

---

### Step 6 — Look for Skew

Check whether one task is significantly slower than others.

```text
Task 1 → 10 sec
Task 2 → 11 sec
Task 3 → 10 sec
Task 4 → 95 sec
```

This can indicate data skew.

---

### Step 7 — Optimize

Depending on the problem:

```text
Small table?
    ↓
Broadcast

Large shuffle?
    ↓
Reduce data / optimize partitioning

Data skew?
    ↓
AQE / salting / skew optimization

Too many columns?
    ↓
Select only required columns

Unnecessary rows?
    ↓
Filter earlier
```

---

# 🧠 Key Takeaways

* Spark chooses a **physical join strategy** rather than simply executing the SQL join directly.
* **Broadcast Hash Join** is useful when one side is small.
* **Sort Merge Join** is suitable for large datasets.
* **Shuffled Hash Join** uses shuffle followed by hash-based matching.
* **Broadcast Nested Loop Join** can be used for certain non-equi join conditions.
* `explain("formatted")` lets you inspect the physical join strategy.
* `BroadcastHashJoin` means Spark is using a broadcast-based strategy.
* `SortMergeJoin` means Spark is using the sort/merge strategy.
* `Exchange` usually indicates data redistribution and is an important signal when investigating shuffle.
* Large shuffle can become one of the biggest costs in a Spark join.
* **Data skew** can create huge partitions and slow down individual tasks.
* **AQE** can dynamically optimize execution at runtime.
* Selecting only required columns reduces the amount of data processed.
* Filtering data before a join can reduce join input size.
* `repartition()` can help organize data by the join key but itself causes a shuffle.
* Never blindly use `broadcast()` or `repartition()` — understand the data first.
* The **Spark UI + `explain()`** combination is extremely useful for diagnosing join performance.

---

# ⭐ Most Important Mental Model

When you see:

```python
df1.join(df2, "id")
```

don't stop at:

> **"Spark is joining two DataFrames."**

Think:

```text
                 JOIN
                   ↓
          Where is the data?
                   ↓
        How are partitions arranged?
                   ↓
          Does Spark need shuffle?
                   ↓
        Which strategy is selected?
                   ↓
       +-----------+-----------+
       |                       |
       ↓                       ↓
 Broadcast                Sort Merge
       |                       |
       ↓                       ↓
 Small Side                Large Data
       |                       |
       ↓                       ↓
 Less Shuffle             Shuffle + Sort
       |                       |
       +-----------+-----------+
                   ↓
              Join Result
```

The key questions for Spark performance are:

> **1. Which join strategy is Spark using?**

> **2. Is Spark shuffling data?**

> **3. How much data is being shuffled?**

> **4. Is the data evenly distributed across partitions?**

> **5. Can the amount of data going into the join be reduced?**

---

# 🚀 Final Mental Model

```text
                 Spark JOIN
                      |
                      ↓
              Logical Operation
                      |
                      ↓
             Catalyst Optimizer
                      |
                      ↓
              Physical Strategy
                      |
          +-----------+-----------+
          |           |           |
          ↓           ↓           ↓
     Broadcast     Sort Merge   Hash Join
          |           |           |
          ↓           ↓           ↓
       Small       Large Data   Suitable
        Side           |        Scenario
          |            ↓
          |         Shuffle
          |            ↓
          |           Sort
          |            ↓
          +------->   JOIN
                       |
                       ↓
                 Spark UI
                       |
          +------------+------------+
          |            |            |
          ↓            ↓            ↓
      Shuffle        Tasks        Skew
       Read/Write   Duration    Detection
```

> **A Spark join is not just a SQL operation. It is a distributed data movement problem.**

Understanding **partitions → shuffle → join strategy → physical plan → Spark UI** is the key to understanding join performance.
