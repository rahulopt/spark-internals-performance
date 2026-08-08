# 15. Repartition in Spark

Partitioning is one of the most important concepts in Spark because Spark processes data **partition by partition**.

In previous lessons, we learned that:

```text
Data
 ↓
Partitions
 ↓
Tasks
 ↓
Executors
```

Now we will understand how we can **change the number or distribution of partitions** using `repartition()`.

---

# 🔹 What is `repartition()`?

`repartition()` is used to redistribute data across partitions.

Example:

```python
df.repartition(10)
```

This tells Spark:

> "Create 10 partitions and redistribute the data across them."

---

# 🔹 Basic Syntax

```python
df.repartition(num_partitions)
```

Example:

```python
df = df.repartition(10)
```

Now Spark will organize the DataFrame into:

```text
Partition 0
Partition 1
Partition 2
...
Partition 9
```

Total:

```text
10 partitions
```

---

# 🔹 Checking Number of Partitions

Use:

```python
df.rdd.getNumPartitions()
```

Example:

```python
print(df.rdd.getNumPartitions())
```

Output:

```text
4
```

After:

```python
df = df.repartition(10)
```

Check again:

```python
print(df.rdd.getNumPartitions())
```

Output:

```text
10
```

---

# 🔹 Why Do We Need Repartition?

Suppose your DataFrame currently has:

```text
4 partitions
```

but your Spark cluster has enough resources to process more tasks.

You can increase the number of partitions:

```python
df = df.repartition(20)
```

Now:

```text
Before:

Partition 0
Partition 1
Partition 2
Partition 3


After:

Partition 0
Partition 1
Partition 2
...
Partition 19
```

This can increase parallelism.

---

# 🔹 `repartition()` Causes Shuffle

This is the most important thing to remember:

> **`repartition()` is a shuffle operation.**

Why?

Because Spark needs to redistribute records across partitions.

Suppose:

```text
Before

Partition 0 → A B C
Partition 1 → D E F
Partition 2 → G H I
```

After:

```python
df.repartition(2)
```

Spark needs to redistribute:

```text
Partition 0 → A C E G I
Partition 1 → B D F H
```

Data has moved between partitions.

Conceptually:

```text
Old Partitions
      ↓
   Shuffle
      ↓
New Partitions
```

---

# 🔹 Why Is Shuffle Expensive?

During a shuffle, Spark may need to:

```text
Read data
   ↓
Partition data
   ↓
Serialize
   ↓
Write shuffle data
   ↓
Transfer across network
   ↓
Read shuffle data
   ↓
Create new partitions
```

This can involve:

* CPU
* Memory
* Disk I/O
* Network I/O

Therefore:

```text
repartition()
     ↓
Shuffle
     ↓
Potentially expensive
```

---

# 🔹 Repartition vs Normal Transformation

Consider:

```python
df.filter("age > 30")
```

A filter is usually a **narrow transformation**.

Data generally stays within the same partition.

```text
Partition 0 → Filter → Partition 0
Partition 1 → Filter → Partition 1
Partition 2 → Filter → Partition 2
```

But:

```python
df.repartition(10)
```

requires redistribution.

```text
Partition 0 ─┐
Partition 1 ─┤
Partition 2 ─┼──→ Shuffle → New Partitions
Partition 3 ─┘
```

Therefore:

```text
filter()
   ↓
Narrow

repartition()
   ↓
Wide
```

---

# 🔹 Repartition to Increase Partitions

Suppose:

```python
df.rdd.getNumPartitions()
```

returns:

```text
4
```

You can increase it:

```python
df = df.repartition(20)
```

Now:

```text
4 partitions
     ↓
   Shuffle
     ↓
20 partitions
```

This can provide more parallelism.

---

# 🔹 Repartition to Decrease Partitions

You can also reduce partitions:

```python
df = df.repartition(2)
```

Example:

```text
20 partitions
      ↓
   Shuffle
      ↓
2 partitions
```

However, if your goal is simply to reduce the number of partitions, `coalesce()` may be more appropriate.

We will compare them later.

---

# 🔹 Repartition by Column

`repartition()` can also distribute data based on a column.

```python
df.repartition("customer_id")
```

Conceptually:

```text
customer_id
      ↓
Hash Partitioning
      ↓
Records with same key
go to the same partition
```

Example:

```text
customer_id = 101
customer_id = 101
customer_id = 101
```

These records will be directed to the same partition under the hash partitioning scheme.

---

# 🔹 Repartition by Number + Column

You can specify both the number of partitions and the column:

```python
df.repartition(
    10,
    "customer_id"
)
```

This means:

```text
Create approximately 10 partitions
+
Distribute data using customer_id
```

This is especially useful when preparing data for operations involving the same key.

---

# 🔹 Example: Repartition Before Join

Suppose:

```python
orders_df.join(
    customers_df,
    "customer_id"
)
```

The join is based on:

```text
customer_id
```

You might repartition:

```python
orders_df = orders_df.repartition(
    "customer_id"
)

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

However, this does **not automatically mean the join will become faster**.

Why?

Because repartitioning itself causes a shuffle.

Spark may already perform the necessary exchange as part of the join.

Therefore:

> **Don't repartition before every join blindly.**

---

# 🔹 Repartition and Number of Tasks

Remember:

```text
1 Partition ≈ 1 Task
```

For a particular stage, Spark generally creates one task for each partition that stage processes.

Suppose:

```python
df = df.repartition(10)
```

Then a subsequent stage may process:

```text
10 partitions
     ↓
10 tasks
```

Conceptually:

```text
Partition 0 → Task 0
Partition 1 → Task 1
Partition 2 → Task 2
...
Partition 9 → Task 9
```

This is why partition count affects parallelism.

---

# 🔹 Too Few Partitions

Suppose:

```text
1 TB Data
+
4 Partitions
```

You might end up with:

```text
250 GB
250 GB
250 GB
250 GB
```

That's a lot of data per partition.

Potential problems:

* Large tasks
* Long execution time
* Poor parallelism
* Possible memory pressure

Increasing partitions may help:

```text
1 TB
 ↓
200 partitions
```

Now each partition contains much less data.

---

# 🔹 Too Many Partitions

More partitions are not always better.

Suppose:

```text
1 GB Data
+
100,000 partitions
```

Now Spark may create a huge number of tiny tasks.

This causes task scheduling overhead.

```text
Too many partitions
       ↓
Too many tasks
       ↓
Scheduling overhead
       ↓
Potentially slower job
```

Therefore:

> **The goal is not to maximize the number of partitions. The goal is to have an appropriate number of partitions.**

---

# 🔹 Repartition Is Not About Splitting Files Only

A common misconception is:

> "`repartition()` just splits the DataFrame into more files."

Not exactly.

It changes the **in-memory/distributed partitioning of the DataFrame**.

If you later write the DataFrame:

```python
df.write.parquet("output/")
```

the number of output files can be influenced by the number of partitions.

For example:

```text
10 partitions
     ↓
Write
     ↓
Approximately 10 output files
```

But the exact output file behavior depends on the write operation and other factors.

---

# 🔹 Repartition Before Writing

Suppose:

```python
df = df.repartition(10)

df.write.parquet(
    "s3a://bucket/output/"
)
```

You may get approximately:

```text
part-00000.parquet
part-00001.parquet
...
part-00009.parquet
```

The exact number and naming can vary depending on the execution.

---

# 🔹 Repartition by Column Before Writing

This is very useful when storing data.

```python
df = df.repartition(
    "region"
)

df.write.parquet(
    "s3a://bucket/output/"
)
```

This distributes the data by `region` before the write.

But note:

```python
repartition("region")
```

is different from:

```python
write.partitionBy("region")
```

These concepts are related but **not the same thing**.

---

# 🔹 `repartition()` vs `partitionBy()`

### `repartition()`

Controls Spark's **execution partitions**.

```python
df.repartition("region")
```

Think:

```text
How should Spark distribute
data across partitions?
```

### `partitionBy()`

Controls the **directory layout of output data**.

```python
df.write.partitionBy("region").parquet(
    "output/"
)
```

Think:

```text
How should output files
be organized in storage?
```

Example:

```text
output/
├── region=North/
├── region=South/
└── region=West/
```

---

# 🔹 Repartitioning and Hash Partitioning

When you write:

```python
df.repartition(
    10,
    "customer_id"
)
```

Spark can use a hash-based partitioning approach.

Conceptually:

```text
customer_id
     ↓
hash(customer_id)
     ↓
partition number
```

For example:

```text
hash(101) → Partition 3
hash(102) → Partition 7
hash(103) → Partition 1
```

The exact hash calculation is an implementation detail.

The important idea is:

> **The join/grouping key determines where records are sent.**

---

# 🔹 Why Same Keys Go to the Same Partition

Suppose:

```text
customer_id = 101
```

appears multiple times:

```text
101
101
101
101
```

When repartitioned by `customer_id`, Spark uses the key to determine the target partition.

Therefore, the same key is directed consistently to the same partition within that partitioning scheme.

This is useful for operations such as:

```text
groupBy(customer_id)
join(customer_id)
```

---

# 🔹 Repartition and `groupBy()`

Consider:

```python
df.groupBy(
    "customer_id"
).count()
```

`groupBy()` requires records with the same key to be brought together.

Therefore, Spark may perform:

```text
Data
 ↓
Shuffle
 ↓
Hash Partitioning
 ↓
Aggregation
```

If you manually do:

```python
df.repartition(
    "customer_id"
)
```

before the `groupBy()`, you are also causing a shuffle.

So blindly doing:

```python
df.repartition("customer_id") \
  .groupBy("customer_id") \
  .count()
```

may not provide a benefit.

Spark can often perform the required exchange itself.

---

# 🔹 Repartition and Caching

Suppose:

```python
df = df.repartition(20).cache()
```

Now the repartitioned result can be cached after it is materialized.

```text
Original Data
     ↓
Repartition
     ↓
20 Partitions
     ↓
Cache
```

Later actions can potentially reuse those cached partitions.

This can be useful if the same repartitioned DataFrame is reused multiple times.

---

# 🔹 Repartition Is Lazy

This is very important.

When you write:

```python
df2 = df.repartition(20)
```

Spark does not immediately perform the shuffle.

Remember:

```text
repartition()
     ↓
Transformation
     ↓
Lazy
```

The shuffle happens when an action triggers execution.

For example:

```python
df2 = df.repartition(20)

df2.count()
```

Now Spark executes the plan.

---

# 🔹 Example Execution

```python
df2 = df.repartition(20)

df2.show()
```

Conceptually:

```text
repartition(20)
      ↓
Logical Plan
      ↓
Physical Plan
      ↓
Shuffle / Exchange
      ↓
20 Partitions
      ↓
show()
```

---

# 🔹 `repartition()` and Spark UI

After executing:

```python
df.repartition(20).count()
```

you can inspect the Spark UI.

Look for:

```text
Exchange
```

and shuffle metrics such as:

```text
Shuffle Write
Shuffle Read
```

You may see a stage boundary because the repartition introduces a shuffle dependency.

---

# 🔹 Example

```python
df2 = df.repartition(20)

df2.count()
```

Conceptually:

```text
Stage 1
   ↓
Read Input
   ↓
Shuffle Write
   ↓
----------------
Stage Boundary
----------------
   ↓
Stage 2
   ↓
Shuffle Read
   ↓
20 Partitions
   ↓
Count
```

The exact physical plan can vary, but the key idea is:

> **Repartition introduces a shuffle dependency.**

---

# 🔹 `repartition()` vs `coalesce()`

This is one of the most important comparisons.

## `repartition()`

```python
df.repartition(10)
```

* Can increase partitions
* Can decrease partitions
* Redistributes data
* Causes a shuffle
* Gives more control over distribution

---

## `coalesce()`

```python
df.coalesce(10)
```

* Primarily used to reduce partitions
* Usually avoids a full shuffle
* Can be more efficient when reducing partitions
* Cannot be used as a general replacement for repartition

---

# 🔹 Example

Suppose:

```text
100 partitions
```

and you want:

```text
10 partitions
```

You could use:

```python
df.repartition(10)
```

but this causes a shuffle.

Or:

```python
df.coalesce(10)
```

which can reduce partitions without a full shuffle.

Therefore:

```text
Need to increase?
        ↓
repartition()

Need to reduce?
        ↓
coalesce() often preferred
```

---

# 🔹 Important Warning

Don't think:

```text
More partitions = More performance
```

The correct idea is:

```text
Appropriate partitions
        ↓
Good parallelism
        ↓
Efficient execution
```

Too few:

```text
Large partitions
     ↓
Poor parallelism
```

Too many:

```text
Tiny partitions
     ↓
Too many tasks
     ↓
Scheduling overhead
```

---

# 🔹 Practical Example

Suppose we have:

```text
100 GB data
```

and currently:

```text
4 partitions
```

Conceptually:

```text
100 GB
 ↓
4 partitions
 ↓
~25 GB each
```

This may be too large depending on the workload and cluster.

We could increase the number:

```python
df = df.repartition(100)
```

Now:

```text
100 GB
 ↓
100 partitions
 ↓
~1 GB average per partition
```

The exact partition sizes may not be equal because data distribution can be uneven.

---

# 🔹 Repartition Does Not Guarantee Equal Data Sizes

This is important.

Suppose:

```python
df.repartition(10)
```

It creates 10 partitions, but that does not mean:

```text
Partition 0 → exactly 10%
Partition 1 → exactly 10%
...
```

Data distribution can be uneven.

Especially when partitioning by a column with skewed values.

Example:

```text
customer_id = 999
```

may appear millions of times.

This can create:

```text
Partition 0 → 5 GB
Partition 1 → 5 GB
Partition 2 → 5 GB
Partition 3 → 50 GB
```

This is **data skew**.

---

# 🔹 Repartition and Data Skew

Repartitioning by a skewed column does not magically solve skew.

For example:

```python
df.repartition(
    "customer_id"
)
```

If one `customer_id` occurs extremely frequently, the same partition can still receive a huge amount of data.

```text
customer_id = 999
        ↓
Hash
        ↓
Partition 7
        ↓
500 million records
```

Therefore:

> **Repartitioning controls distribution, but the distribution depends on the key.**

---

# 🔹 When Should You Use `repartition()`?

Good use cases include:

### 1. Increasing parallelism

```python
df.repartition(100)
```

when the current partition count is too low.

### 2. Preparing data for a downstream operation

```python
df.repartition(
    "customer_id"
)
```

when there is a justified reason to organize data around a key.

### 3. Controlling output file count

```python
df.repartition(20).write.parquet(...)
```

when you need a more appropriate number of output tasks/files.

### 4. Fixing an unsuitable partition distribution

When profiling shows that the current distribution is poor.

---

# 🔹 When Should You NOT Use It?

Avoid unnecessary patterns such as:

```python
df.repartition(1000)
```

just because:

> "More partitions means faster Spark."

Also avoid:

```python
df.repartition("customer_id")
```

before every join/groupBy without checking whether Spark already performs the necessary shuffle.

Remember:

```text
repartition()
    ↓
shuffle
    ↓
cost
```

---

# 🔹 Practical Debugging

When deciding whether to repartition, inspect:

```python
df.rdd.getNumPartitions()
```

Then inspect the physical plan:

```python
df.explain("formatted")
```

After execution, inspect Spark UI for:

```text
Shuffle Read
Shuffle Write
Task Duration
Partition Size
```

This gives you evidence instead of guessing.

---

# 🔹 Simple Decision Tree

```text
Do I have too few partitions?
          |
         Yes
          ↓
   repartition()

Do I need fewer partitions?
          |
         Yes
          ↓
   coalesce() often

Do I need data distributed by a key?
          |
         Yes
          ↓
repartition(key)

Is Spark already shuffling for
the next operation?
          |
         Yes
          ↓
Avoid unnecessary repartition()
```

---

# 🧠 Key Takeaways

* `repartition()` changes the partitioning of a DataFrame.
* `repartition()` is a **transformation**, so it is lazy.
* `repartition()` causes a **shuffle**.
* Shuffle means data is redistributed between partitions.
* `repartition(n)` creates `n` target partitions.
* `repartition(n, "column")` distributes data using the specified column.
* Repartitioning can increase or decrease the number of partitions.
* More partitions do **not** automatically mean better performance.
* Too few partitions can reduce parallelism.
* Too many partitions can create task scheduling overhead.
* Repartitioning by a key can be useful for joins and aggregations, but it is not automatically beneficial.
* `repartition()` does not guarantee equal-sized partitions.
* A skewed partitioning key can still create data skew.
* `repartition()` and `write.partitionBy()` solve different problems.
* `repartition()` controls Spark's execution partitioning.
* `partitionBy()` controls the output directory layout.
* `repartition()` and `coalesce()` are not interchangeable.
* `coalesce()` is generally preferred when simply reducing the number of partitions.
* Always check the physical plan and Spark UI before adding repartitioning for performance.

---

# ⭐ Most Important Mental Model

Don't think:

```text
repartition(10)
      ↓
"Make 10 pieces"
```

Think:

```text
repartition(10)
      ↓
Redistribute the data
      ↓
Shuffle
      ↓
Create 10 target partitions
```

And:

```text
repartition("customer_id")
          ↓
   Hash Partitioning
          ↓
Same keys → same target partition
```

---

# 🚀 Final Picture

```text
                    DATA
                      |
                      ↓
                Partitions
                      |
              +-------+-------+
              |               |
              ↓               ↓
         Too Few?          Too Many?
              |               |
              ↓               ↓
        repartition()      coalesce()
              |
              ↓
           Shuffle
              |
              ↓
      New Partition Layout
              |
              ↓
       More Appropriate
          Parallelism
```

> **`repartition()` is powerful, but it is not free. Every repartition can introduce a shuffle, so use it when you have a clear reason to redistribute the data.**
