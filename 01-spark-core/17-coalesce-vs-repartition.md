# 17. Coalesce vs Repartition

In the previous lessons, we learned:

* `repartition()` changes the number and distribution of partitions.
* `repartition()` causes a shuffle.
* Too many or too few partitions can affect performance.

Now we will compare:

```python
repartition()
```

vs

```python
coalesce()
```

This is one of the most important partitioning concepts in Spark.

---

# 🔹 The Simple Difference

The easiest way to remember:

```text
repartition()
    ↓
Redistribute data
    ↓
Shuffle
```

while:

```text
coalesce()
    ↓
Reduce partitions
    ↓
Usually avoids full shuffle
```

So:

> **Use `repartition()` when you need redistribution.**
>
> **Use `coalesce()` when you mainly want to reduce partitions.**

---

# 🔹 Basic Syntax

### Repartition

```python
df.repartition(10)
```

### Coalesce

```python
df.coalesce(10)
```

Both return a new DataFrame.

---

# 🔹 Both Are Lazy

Neither operation immediately executes.

For example:

```python
df2 = df.repartition(10)
```

does not immediately perform the shuffle.

Similarly:

```python
df2 = df.coalesce(10)
```

does not immediately execute the operation.

They are transformations.

```text
repartition()
      ↓
Transformation
      ↓
Lazy

coalesce()
      ↓
Transformation
      ↓
Lazy
```

An action such as:

```python
df2.count()
```

is required to trigger execution.

---

# 🔹 Repartition

Suppose we have:

```text
4 partitions
```

and write:

```python
df = df.repartition(8)
```

Spark redistributes the data.

```text
Before:

P0
P1
P2
P3

        ↓
      Shuffle
        ↓

P0
P1
P2
P3
P4
P5
P6
P7
```

The important point is:

> Data is allowed to move between partitions.

---

# 🔹 Coalesce

Suppose we have:

```text
8 partitions
```

and write:

```python
df = df.coalesce(4)
```

Spark tries to combine existing partitions.

Conceptually:

```text
Before:

P0 ─┐
P1 ─┘
     ↓
    P0'

P2 ─┐
P3 ─┘
     ↓
    P1'

P4 ─┐
P5 ─┘
     ↓
    P2'

P6 ─┐
P7 ─┘
     ↓
    P3'
```

The important idea is:

> Coalesce reduces the number of partitions without requiring a full redistribution of all records.

---

# 🔹 The Biggest Difference: Shuffle

This is the most important comparison.

## `repartition()`

```python
df.repartition(4)
```

```text
Old partitions
      ↓
   Shuffle
      ↓
New partitions
```

---

## `coalesce()`

```python
df.coalesce(4)
```

```text
Old partitions
      ↓
Combine partitions
      ↓
Fewer partitions
```

Therefore:

| Operation       |                 Shuffle | Increase Partitions | Decrease Partitions |
| --------------- | ----------------------: | ------------------: | ------------------: |
| `repartition()` |                     Yes |                   ✅ |                   ✅ |
| `coalesce()`    | Usually no full shuffle |                   ❌ |                   ✅ |

---

# 🔹 Why Coalesce Is Usually Faster

Suppose:

```text
100 partitions
```

and you want:

```text
10 partitions
```

With repartition:

```text
100 partitions
      ↓
Shuffle
      ↓
10 partitions
```

Data can move across the cluster.

With coalesce:

```text
100 partitions
      ↓
Combine existing partitions
      ↓
10 partitions
```

There is no need for a full redistribution.

Therefore:

```text
coalesce()
    ↓
Less data movement
    ↓
Usually cheaper
```

---

# 🔹 Important Limitation of Coalesce

`coalesce()` is mainly designed to **reduce** partitions.

For example:

```python
df.coalesce(10)
```

when the current number is:

```text
100
```

makes sense.

But:

```python
df.coalesce(100)
```

when the current number is:

```text
10
```

does not give you 100 useful partitions.

In other words:

> **Coalesce should not be used to increase parallelism.**

If you need more partitions:

```python
df.repartition(100)
```

---

# 🔹 Example

Suppose:

```python
df.rdd.getNumPartitions()
```

returns:

```text
100
```

You want:

```text
20
```

### Option 1

```python
df2 = df.repartition(20)
```

### Option 2

```python
df2 = df.coalesce(20)
```

Both can produce:

```text
20 partitions
```

But the execution behavior is different.

```text
repartition(20)
       ↓
Shuffle
       ↓
20 partitions


coalesce(20)
       ↓
Reduce existing partitions
       ↓
20 partitions
```

If you simply want to reduce partitions, `coalesce()` is usually the better choice.

---

# 🔹 When Repartition Is Better

Suppose:

```text
10 partitions
```

but you need:

```text
100 partitions
```

Use:

```python
df.repartition(100)
```

because:

```python
df.coalesce(100)
```

is not designed to increase the partition count.

---

# 🔹 Repartition Can Also Decrease Partitions

This is important.

People sometimes think:

```text
repartition() → increase
coalesce()    → decrease
```

That's not completely correct.

`repartition()` can do both:

```python
df.repartition(100)
```

or:

```python
df.repartition(10)
```

It always performs redistribution.

So:

```text
repartition()
    ↓
Increase OR decrease
    ↓
Shuffle
```

while:

```text
coalesce()
    ↓
Reduce
    ↓
Usually no full shuffle
```

---

# 🔹 Why Would We Use Repartition to Decrease?

Sometimes simply combining existing partitions isn't enough.

Suppose the existing partitions are badly distributed:

```text
P0 → 10 GB
P1 → 10 GB
P2 → 10 GB
P3 → 10 GB
P4 → 1 GB
P5 → 1 GB
P6 → 1 GB
P7 → 1 GB
```

You want:

```text
4 partitions
```

Using:

```python
df.coalesce(4)
```

may preserve some imbalance because coalesce avoids a full redistribution.

Using:

```python
df.repartition(4)
```

allows Spark to redistribute records across the new partitions.

```text
Old uneven distribution
        ↓
     Shuffle
        ↓
New distribution
        ↓
More balanced
```

So:

> **If you need redistribution, use `repartition()`, even when decreasing the partition count.**

---

# 🔹 Real-World Example: Writing Output Files

Suppose your DataFrame has:

```text
5000 partitions
```

and you want fewer output files.

You could use:

```python
df.coalesce(100)
```

before writing:

```python
df.coalesce(100).write.parquet(
    "s3a://bucket/orders/"
)
```

Conceptually:

```text
5000 partitions
       ↓
coalesce(100)
       ↓
100 partitions
       ↓
Write
       ↓
~100 output files
```

This is a common practical use case.

---

# 🔹 Why Not Always Use Repartition?

You could write:

```python
df.repartition(100).write.parquet(...)
```

but now:

```text
5000 partitions
       ↓
Shuffle
       ↓
100 partitions
       ↓
Write
```

You have introduced a potentially expensive shuffle just to reduce the number of output files.

If redistribution isn't required, `coalesce()` may be more efficient.

---

# 🔹 Example: Filtering Data

Suppose:

```python
filtered_df = df.filter(
    df.country == "India"
)
```

Input:

```text
1000 partitions
```

But after filtering, the resulting dataset is much smaller.

You might decide:

```python
filtered_df = filtered_df.coalesce(100)
```

Then write:

```python
filtered_df.write.parquet(
    "output/"
)
```

Conceptually:

```text
Large Dataset
     ↓
Filter
     ↓
Small Dataset
     ↓
100 partitions
     ↓
Write
```

This can help avoid generating thousands of tiny output files.

---

# 🔹 Repartition Before a Join

Suppose:

```python
orders_df.join(
    customers_df,
    "customer_id"
)
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

But be careful.

The join itself may require a shuffle.

So manually repartitioning both DataFrames can create unnecessary work.

Always inspect:

```python
joined_df.explain("formatted")
```

before adding repartitioning for performance.

---

# 🔹 Repartition by Key

`repartition()` can use a column:

```python
df.repartition(
    "customer_id"
)
```

or:

```python
df.repartition(
    100,
    "customer_id"
)
```

This means Spark distributes the records based on the key.

Conceptually:

```text
customer_id
      ↓
Hash
      ↓
Partition
```

This is useful when you specifically need data distributed by a key.

`coalesce()` does not provide the same key-based redistribution behavior.

---

# 🔹 Coalesce Does Not Fix Data Skew

Suppose:

```text
P0 → 1 GB
P1 → 1 GB
P2 → 1 GB
P3 → 50 GB
```

If you do:

```python
df.coalesce(2)
```

you may end up with something like:

```text
P0' → 2 GB
P1' → 52 GB
```

The imbalance may remain.

Coalesce is not a skew-solving mechanism.

If you need redistribution:

```python
df.repartition(2)
```

can redistribute records.

---

# 🔹 A Simple Analogy

Imagine you have 8 boxes:

```text
Box 1
Box 2
Box 3
Box 4
Box 5
Box 6
Box 7
Box 8
```

and want 4 boxes.

### Coalesce

You combine existing boxes:

```text
1 + 2 → New Box
3 + 4 → New Box
5 + 6 → New Box
7 + 8 → New Box
```

No need to individually redistribute every item.

---

### Repartition

You open all boxes and redistribute the items:

```text
8 boxes
   ↓
Move items around
   ↓
4 new boxes
```

This takes more work, but you get more control over distribution.

---

# 🔹 Performance Comparison

| Feature                              | `repartition()` | `coalesce()` |
| ------------------------------------ | --------------- | ------------ |
| Increase partitions                  | ✅               | ❌            |
| Decrease partitions                  | ✅               | ✅            |
| Full shuffle                         | ✅               | Usually no   |
| Redistribute data                    | ✅               | Limited      |
| Key-based partitioning               | ✅               | ❌            |
| Good for increasing parallelism      | ✅               | ❌            |
| Good for reducing output files       | Sometimes       | ✅            |
| Usually cheaper for simple reduction | ❌               | ✅            |

---

# 🔹 Execution Example

Suppose:

```python
df = df.repartition(100)
```

Then:

```python
df.count()
```

Conceptually:

```text
Stage 1
   ↓
Read Data
   ↓
Shuffle Write
   ↓
----------------
Shuffle Boundary
----------------
   ↓
Stage 2
   ↓
Shuffle Read
   ↓
100 partitions
   ↓
Count
```

---

Now:

```python
df = df.coalesce(20)
```

Then:

```python
df.count()
```

Conceptually:

```text
Existing Partitions
       ↓
Coalesce
       ↓
20 partitions
       ↓
Count
```

The exact physical execution can vary, but the key difference is that coalesce generally avoids the full redistribution that repartition performs.

---

# 🔹 Spark UI

When investigating these operations, open Spark UI.

For:

```python
df.repartition(100).count()
```

look for:

```text
Exchange
Shuffle Write
Shuffle Read
```

You should expect shuffle-related work.

For:

```python
df.coalesce(20).count()
```

you generally won't see the same full shuffle exchange caused by repartition.

---

# 🔹 Important: `coalesce()` Is Not Always Better

It is tempting to say:

```text
coalesce() = good
repartition() = bad
```

That's wrong.

They solve different problems.

Example:

```text
Current:
100 partitions

Problem:
Data is badly distributed

Required:
20 balanced partitions
```

In this case:

```python
df.repartition(20)
```

may be better than:

```python
df.coalesce(20)
```

because redistribution is actually required.

---

# 🔹 Practical Decision Tree

Use this mental model:

```text
Do I need MORE partitions?
        |
       Yes
        ↓
   repartition()


Do I need FEWER partitions?
        |
       Yes
        ↓
Do I need to redistribute
the data?
        |
     +--+--+
    No     Yes
    |       |
    ↓       ↓
coalesce  repartition
```

This is probably the easiest way to remember it.

---

# 🔹 Example 1

Current:

```text
4 partitions
```

Need:

```text
20 partitions
```

Use:

```python
df.repartition(20)
```

---

# 🔹 Example 2

Current:

```text
1000 partitions
```

Need:

```text
100 partitions
```

Only because you want fewer output files.

Use:

```python
df.coalesce(100)
```

---

# 🔹 Example 3

Current:

```text
1000 partitions
```

Need:

```text
100 balanced partitions
```

because the current distribution is badly skewed.

Use:

```python
df.repartition(100)
```

because redistribution is required.

---

# 🔹 Example 4

Need data distributed by:

```text
customer_id
```

Use:

```python
df.repartition(
    100,
    "customer_id"
)
```

Not:

```python
df.coalesce(100)
```

---

# 🔹 Common Mistake

### ❌ Wrong Thinking

```text
Need fewer partitions
      ↓
Always coalesce
```

Not always.

Ask:

> **Do I need redistribution?**

If yes:

```python
repartition()
```

If no:

```python
coalesce()
```

---

# 🔹 Another Common Mistake

### ❌

```python
df.repartition(1000)
```

just because:

```text
"More partitions = more parallelism = faster"
```

This can create:

```text
Too many tasks
     ↓
Scheduling overhead
     ↓
Slower job
```

Always consider:

* Data size
* Partition size
* Number of executor cores
* Task duration
* Shuffle cost
* Data skew

---

# 🔹 Real-World ETL Example

Imagine an ETL pipeline:

```text
S3
 ↓
Read 2 TB
 ↓
Filter
 ↓
Join
 ↓
Aggregation
 ↓
Write Parquet
```

After filtering:

```text
2 TB → 100 GB
```

But the DataFrame still has:

```text
2000 partitions
```

You want to write a manageable number of files.

A reasonable option might be:

```python
filtered_df = filtered_df.coalesce(200)

filtered_df.write.parquet(
    "s3a://bucket/output/"
)
```

Why?

```text
2000 partitions
      ↓
coalesce
      ↓
200 partitions
      ↓
Write
```

No unnecessary full redistribution just for reducing output parallelism.

---

# 🔹 Another ETL Example

Now imagine:

```text
100 GB Data
 ↓
Current: 10 partitions
 ↓
Expensive downstream processing
```

The cluster has plenty of available cores.

You want more parallelism.

Use:

```python
df = df.repartition(100)
```

Because:

```text
10 partitions
    ↓
Shuffle
    ↓
100 partitions
    ↓
More parallelism
```

Coalesce cannot solve this.

---

# 🔹 One-Line Memory Trick

Remember:

```text
                 REPARTITION
                      |
          +-----------+-----------+
          |                       |
       Increase                Decrease
          |                       |
          +-----------+-----------+
                      |
                    Shuffle


                  COALESCE
                      |
                   Decrease
                      |
                Usually no
                full shuffle
```

Or even simpler:

> **Repartition = Redistribute**
>
> **Coalesce = Combine**

---

# 🧠 Key Takeaways

* Both `repartition()` and `coalesce()` change the number of partitions.
* Both are lazy transformations.
* `repartition()` causes a shuffle.
* `coalesce()` generally avoids a full shuffle.
* `repartition()` can increase or decrease partitions.
* `coalesce()` is mainly used to decrease partitions.
* Use `repartition()` when you need data redistribution.
* Use `coalesce()` when you simply want fewer partitions.
* `repartition()` is useful for key-based partitioning.
* `coalesce()` does not perform the same key-based redistribution.
* `coalesce()` is often useful before writing output to avoid excessive small files.
* `repartition()` can be better than `coalesce()` when existing data is badly distributed.
* Neither operation should be added blindly for performance.
* Always consider shuffle cost.
* Use `explain("formatted")` and Spark UI to verify what actually happens.

---

# ⭐ Final Mental Model

```text
                    Current Partitions
                           |
                           ↓
                  What do I need?
                           |
             +-------------+-------------+
             |                           |
        MORE partitions             FEWER partitions
             |                           |
             ↓                           ↓
       repartition()             Need redistribution?
                                         |
                              +----------+----------+
                              |                     |
                             No                    Yes
                              |                     |
                              ↓                     ↓
                         coalesce()          repartition()
```

The simplest rule:

```python
# Need more partitions
df.repartition(100)

# Need fewer partitions, no redistribution needed
df.coalesce(20)

# Need fewer partitions + redistribution
df.repartition(20)

# Need partitioning by a key
df.repartition(100, "customer_id")
```

> **`coalesce()` is about reducing partitions efficiently. `repartition()` is about redistributing data, even if that means paying the cost of a shuffle.**
