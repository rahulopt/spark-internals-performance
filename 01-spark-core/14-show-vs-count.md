# 14. `show()` vs `count()`

At first glance, both `show()` and `count()` look like simple DataFrame operations.

But internally, they behave very differently.

Understanding this difference is important for understanding **Spark execution, jobs, stages, tasks, and performance**.

---

## 🔹 `show()`

`show()` is used to display rows from a DataFrame.

```python
df.show()
```

By default, Spark displays the first **20 rows**.

```python
df.show(10)
```

displays the first 10 rows.

You can also prevent truncation:

```python
df.show(10, truncate=False)
```

---

## 🔹 `count()`

`count()` returns the total number of rows in a DataFrame.

```python
df.count()
```

Example:

```text
1000000
```

Unlike `show()`, `count()` needs to determine the total number of rows.

---

# 🔹 Both Are Actions

This is the first important point.

Both:

```python
df.show()
```

and:

```python
df.count()
```

are **actions**.

Therefore, both can trigger Spark job execution.

```text
Transformation
     ↓
Lazy Evaluation
     ↓
Action
     ↓
Job Execution
```

For example:

```python
filtered_df = df.filter(
    df.age > 25
)
```

Nothing is executed yet.

Then:

```python
filtered_df.show()
```

triggers execution.

Similarly:

```python
filtered_df.count()
```

also triggers execution.

---

# 🔹 `show()` vs `count()`

The key difference is **what result Spark needs to produce**.

### `show()`

Spark needs enough data to display the requested rows.

```python
df.show()
```

Conceptually:

```text
DataFrame
   ↓
Find rows needed for display
   ↓
Return rows
   ↓
Driver displays them
```

### `count()`

Spark needs the total number of rows.

```python
df.count()
```

Conceptually:

```text
Partition 0 → count
Partition 1 → count
Partition 2 → count
Partition 3 → count
       ↓
Combine counts
       ↓
Total count
```

---

# 🔹 Why `count()` Can Be Expensive

Suppose we have:

```text
1 TB Dataset
```

and:

```python
df.count()
```

Spark needs to determine how many rows exist.

Conceptually:

```text
Partition 0 → 10M rows
Partition 1 → 15M rows
Partition 2 → 12M rows
Partition 3 → 18M rows
...
       ↓
Aggregate all partition counts
       ↓
Total
```

Therefore, Spark may need to process the entire dataset.

---

# 🔹 Why `show()` Can Be Cheaper

Suppose:

```python
df.show(20)
```

Spark only needs enough rows to display the requested output.

Conceptually:

```text
Partition 0
    ↓
Find rows
    ↓
Enough rows?
    ↓
Yes
    ↓
Return result
```

However, this does **not** mean `show()` is always cheap.

The actual work depends on the transformations before `show()`.

---

# 🔹 Important Example

Consider:

```python
df.filter(
    df.age > 30
).show()
```

Spark needs to find rows satisfying:

```text
age > 30
```

If matching rows are available early in the scan, Spark may obtain enough rows without processing everything.

But if the operation requires examining all data first, `show()` may still require substantial work.

For example:

```python
df.groupBy("department").count().show()
```

The aggregation requires processing the input before Spark can produce the grouped result.

So:

> **`show()` does not magically mean "only read 20 rows from the original dataset."**

---

# 🔹 `show()` After a Filter

Consider:

```python
filtered_df = df.filter(
    df.salary > 100000
)

filtered_df.show()
```

Spark needs to find rows where:

```text
salary > 100000
```

If the first partition contains enough matching rows, Spark may not need to fully process every partition just to display the requested number of rows.

Conceptually:

```text
Partition 0
     ↓
Matching rows found
     ↓
Enough rows?
     ↓
Yes
     ↓
show()
```

---

# 🔹 `count()` After a Filter

Now:

```python
filtered_df.count()
```

Spark needs the total number of matching rows.

Therefore:

```text
Partition 0 → count matches
Partition 1 → count matches
Partition 2 → count matches
Partition 3 → count matches
        ↓
      SUM
        ↓
Total
```

This generally requires processing all relevant input partitions.

---

# 🔹 `show()` Does Not Mean One Task

A common misconception is:

> "`show()` only displays 20 rows, so Spark will create only one task."

Not necessarily.

The number of tasks depends on the execution plan and the partitions that need to be processed.

For example:

```python
df.filter(
    df.age > 30
).show()
```

Spark may need to examine multiple partitions to find enough rows.

```text
Partition 0 ──┐
Partition 1 ──┤
Partition 2 ──┼──> show()
Partition 3 ──┤
Partition 4 ──┘
```

---

# 🔹 `count()` and Partitions

For:

```python
df.count()
```

Spark generally needs to process all relevant partitions.

```text
Partition 0 → count
Partition 1 → count
Partition 2 → count
Partition 3 → count
Partition 4 → count
        ↓
     Combine
        ↓
      Result
```

This is why `count()` is often more expensive than simply inspecting a few rows.

---

# 🔹 `show()` and `limit()`

`show()` behaves conceptually like requesting a limited number of rows.

For example:

```python
df.show(20)
```

is effectively asking Spark:

> "Give me up to 20 rows to display."

You can explicitly use:

```python
df.limit(20).show()
```

This makes the limit part of the DataFrame plan.

---

# 🔹 `show()` vs `collect()`

These are different.

### `show()`

```python
df.show()
```

Displays rows in a readable tabular format.

### `collect()`

```python
rows = df.collect()
```

Brings **all rows** to the driver.

This can be dangerous for large datasets.

```text
Large DataFrame
      ↓
collect()
      ↓
All rows → Driver Memory
```

Therefore:

> Avoid `collect()` on large DataFrames.

For inspecting data, prefer:

```python
df.show()
```

or:

```python
df.limit(20).collect()
```

when you specifically need Python objects.

---

# 🔹 `show()` vs `take()`

`take()` returns rows to the driver.

```python
df.take(10)
```

Result:

```python
[
    Row(...),
    Row(...),
    ...
]
```

While:

```python
df.show(10)
```

prints them in a table.

So:

```text
show()
    ↓
Display rows

take()
    ↓
Return rows to Python
```

---

# 🔹 `count()` Is Not a Metadata Operation

Another common misconception:

> "`count()` just checks metadata."

Usually, no.

For a DataFrame:

```python
df.count()
```

Spark generally needs to execute the relevant computation to determine the row count.

For example:

```python
df.filter(
    df.age > 30
).count()
```

The count depends on the result of the filter.

Spark cannot simply look at the original file metadata and know how many rows satisfy the filter.

---

# 🔹 Example With a Transformation

```python
result_df = (
    orders_df
    .filter("amount > 1000")
    .select("customer_id", "amount")
)
```

Nothing runs yet.

### Using `show()`

```python
result_df.show()
```

Spark executes the plan to produce rows for display.

### Using `count()`

```python
result_df.count()
```

Spark executes the plan to calculate the total number of rows.

---

# 🔹 Important: Both Can Execute the Same Transformation Again

Consider:

```python
filtered_df = df.filter(
    df.amount > 1000
)

filtered_df.show()

filtered_df.count()
```

You have two actions:

```text
show()
  ↓
Job 1

count()
  ↓
Job 2
```

Unless the DataFrame is cached/persisted appropriately, Spark may recompute the transformation for the second action.

---

# 🔹 Caching Changes the Situation

Suppose:

```python
filtered_df = df.filter(
    df.amount > 1000
).cache()
```

Then:

```python
filtered_df.show()
filtered_df.count()
```

The first action computes and caches the data.

```text
First Action
     ↓
Compute
     ↓
Cache
```

The next action can reuse the cached data.

```text
Second Action
     ↓
Read cached data
     ↓
Count
```

This can avoid recomputing the expensive transformation.

---

# 🔹 Example

Without cache:

```python
filtered_df = df.filter(
    df.amount > 1000
)

filtered_df.show()
filtered_df.count()
```

Conceptually:

```text
show()
 ↓
Read → Filter
 ↓

count()
 ↓
Read → Filter
```

With cache:

```python
filtered_df = df.filter(
    df.amount > 1000
).cache()

filtered_df.show()
filtered_df.count()
```

Conceptually:

```text
show()
 ↓
Read → Filter → Cache

count()
 ↓
Read Cache
```

---

# 🔹 `show()` and Wide Transformations

Consider:

```python
df.groupBy("customer_id").count().show()
```

Even though `show()` only displays a few rows, the `groupBy()` is a **wide transformation**.

Spark needs to perform the required aggregation and shuffle before it can produce the grouped result.

```text
Input
  ↓
groupBy()
  ↓
Shuffle
  ↓
Aggregation
  ↓
show()
```

So:

> **The cost of `show()` depends heavily on the transformations before it.**

---

# 🔹 `count()` and Wide Transformations

Similarly:

```python
df.groupBy("customer_id").count().count()
```

The inner `count()` is part of the aggregation, while the outer `count()` counts the resulting grouped rows.

This can require:

```text
Input
  ↓
Shuffle
  ↓
Aggregation
  ↓
Count grouped results
```

The important point is that actions do not determine the entire cost by themselves.

The **lineage and physical plan** determine the actual work.

---

# 🔹 Use `explain()` to Understand the Work

Whenever you are unsure about what Spark is doing:

```python
df.explain("formatted")
```

For example:

```python
df.filter(
    df.age > 30
).show()
```

and:

```python
df.filter(
    df.age > 30
).count()
```

may have similar underlying transformations but different execution requirements for producing their final results.

---

# 🔹 Practical Comparison

| Operation   | Purpose            | Returns      | Typical Work                        |
| ----------- | ------------------ | ------------ | ----------------------------------- |
| `show()`    | Display rows       | Nothing      | Limited output, depends on plan     |
| `count()`   | Count rows         | Integer      | Usually processes all relevant data |
| `take(n)`   | Get first `n` rows | List of Rows | Limited output                      |
| `collect()` | Get all rows       | List of Rows | Processes and transfers all rows    |

---

# 🔹 When Should You Use `show()`?

Use `show()` when:

* Exploring a DataFrame
* Checking whether transformations worked
* Debugging data
* Inspecting schema/data values
* Quickly checking output

Example:

```python
df.show(10, truncate=False)
```

---

# 🔹 When Should You Use `count()`?

Use `count()` when:

* You actually need the number of rows
* Validating record counts
* Checking filtering logic
* Comparing input/output record counts

Example:

```python
print(df.count())
```

But avoid using `count()` unnecessarily just to "see if the DataFrame works."

---

# ⚠️ Common Mistake

A beginner often writes:

```python
df.show()
df.count()
df.show()
df.count()
```

This can trigger multiple jobs.

```text
show()
  ↓
Job 1

count()
  ↓
Job 2

show()
  ↓
Job 3

count()
  ↓
Job 4
```

If the DataFrame is expensive to compute, this can become costly.

---

# 🔹 Better Debugging Pattern

Instead of repeatedly triggering actions:

```python
df.show()
df.count()
df.show()
```

inspect what you actually need.

For example:

```python
df.printSchema()
df.show(10, truncate=False)
```

If you genuinely need the count:

```python
df.count()
```

And if the same expensive DataFrame will be used repeatedly:

```python
df = df.cache()
```

Then perform the required actions.

---

# 🧠 Key Takeaways

* `show()` and `count()` are both **actions**.
* Both can trigger Spark job execution.
* `show()` is mainly used to **inspect data**.
* `count()` is used to **calculate the total number of rows**.
* `count()` generally needs to process all relevant partitions.
* `show()` may not need to process the entire dataset when only a limited number of rows are required.
* However, `show()` can still be expensive if previous transformations require a full computation.
* `show()` does **not** guarantee only one task.
* `collect()` is different from `show()` because it brings all rows to the driver.
* Calling multiple actions on the same uncached DataFrame can cause **recomputation**.
* `cache()`/`persist()` can help when the same expensive DataFrame is reused.
* The cost of an action depends heavily on the **transformations and physical plan before it**.
* Use `explain("formatted")` and Spark UI when you want to understand the actual execution.

---

# ⭐ Most Important Mental Model

Don't think:

```text
show()  = cheap
count() = expensive
```

Instead think:

```text
Action
  ↓
What does Spark need to produce?
  ↓
What transformations are before it?
  ↓
Does it need all partitions?
  ↓
Does it require shuffle?
  ↓
How expensive is the physical plan?
```

The better mental model is:

> **`show()` and `count()` are just actions. The actual cost comes from the work Spark must perform to produce their results.**

---

## 🔥 Quick Example

```python
df = (
    orders_df
    .filter("amount > 1000")
    .select("customer_id", "amount")
)

df.show(10)

df.count()
```

Think about the execution as:

```text
             Data
              ↓
           Filter
              ↓
           Select
              ↓
        +-----+-----+
        |           |
        ↓           ↓
      show()      count()
        |           |
   Limited rows   All rows
```

And if the transformation contains a shuffle:

```text
Data
 ↓
Filter
 ↓
GroupBy / Join
 ↓
Shuffle
 ↓
Aggregation
 ↓
Action
```

then even `show()` can involve significant work.

---

## 🚀 Final Takeaway

```text
show()
  ↓
"Show me some rows."

count()
  ↓
"Tell me how many rows exist."

Both are Actions
      ↓
Both can trigger Jobs
      ↓
Actual cost depends on
the execution plan
```

> **Never judge Spark performance just by looking at the action. Always look at the complete execution plan.**
