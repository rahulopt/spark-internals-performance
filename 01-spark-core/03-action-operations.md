# Action Operations and Utility Methods

## Introduction

Spark operations can be broadly categorized into three groups:

- **Transformations**
- **Actions**
- **Utility Methods**

Transformations define **what** needs to be done with the data, while Actions trigger Spark to execute the computation. Utility methods help inspect or understand a DataFrame during development and debugging.

Spark follows **Lazy Evaluation**, meaning transformations are not executed until an Action is called.

---

# What is an Action?

An **Action** is an operation that triggers the execution of a Spark application.

When an Action is invoked, Spark:

1. Analyzes the DAG (Directed Acyclic Graph)
2. Optimizes the execution plan
3. Creates one or more Jobs
4. Divides Jobs into Stages
5. Splits Stages into Tasks
6. Sends Tasks to Executors
7. Returns or writes the final result

```
Transformations
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
      Result
```

---

# Why Do We Need Actions?

Transformations alone do not execute any computation. They simply build a logical execution plan.

Example:

```python
orders = spark.read.csv("orders.csv", header=True)

filtered_orders = orders.filter("quantity > 2")

selected_orders = filtered_orders.select("customer_id", "quantity")
```

At this point, Spark has not executed anything.

Once an Action is called:

```python
selected_orders.show()
```

Spark executes the entire pipeline.

---

# Common Action Operations

## show()

Displays the contents of a DataFrame.

```python
orders.show()
```

Useful for quickly previewing the data.

---

## count()

Returns the total number of rows.

```python
orders.count()
```

Spark scans the entire dataset to calculate the count.

---

## collect()

Returns all rows to the Driver as a Python list.

```python
rows = orders.collect()
```

⚠️ **Avoid using `collect()` on large datasets** because all data is transferred to the Driver, which can lead to memory issues.

---

## take()

Returns the first **N** rows.

```python
orders.take(5)
```

Useful when you only need a small sample of the data.

---

## first()

Returns the first row of the DataFrame.

```python
orders.first()
```

---

## head()

Returns the first row or the first **N** rows.

```python
orders.head()

orders.head(10)
```

---

## foreach()

Executes a function for every row in the DataFrame.

```python
orders.foreach(print)
```

Typically used for custom processing.

---

## write()

Writes the processed data to an external storage system.

```python
orders.write.mode("overwrite").parquet("output/")
```

Supported output formats include:

- CSV
- JSON
- Parquet
- ORC
- JDBC
- Hive
- Amazon S3

Since Spark must execute all pending transformations before saving the data, `write()` is also an Action.

---

# Utility Methods

Not every DataFrame method is a Transformation or an Action.

Spark provides several **utility methods** that help inspect and understand a DataFrame. These methods are commonly used for debugging, exploration, and development.

| Method | Purpose |
|---------|---------|
| `printSchema()` | Displays the DataFrame schema |
| `explain()` | Displays the execution plan |
| `columns` | Returns the list of column names |
| `dtypes` | Returns column names with their data types |
| `schema` | Returns the schema object |
| `inputFiles()` | Returns the input file paths |

---

## printSchema()

Displays the schema of the DataFrame.

```python
orders.printSchema()
```

Example output:

```text
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
```

This method is useful for verifying column names and data types before performing transformations.

---

## explain()

Displays Spark's execution plan.

```python
orders.filter("quantity > 2").explain()
```

Example output:

```text
== Physical Plan ==
*(1) Filter (quantity > 2)
+- FileScan csv ...
```

`explain()` is one of the most useful debugging tools because it shows how Spark plans to execute the query.

---

## columns

Returns the names of all columns.

```python
orders.columns
```

Output:

```python
['order_id', 'customer_id', 'quantity', 'unit_price']
```

---

## dtypes

Returns the data type of each column.

```python
orders.dtypes
```

Output:

```python
[
 ('order_id', 'string'),
 ('customer_id', 'string'),
 ('quantity', 'int'),
 ('unit_price', 'double')
]
```

---

## schema

Returns the schema as a `StructType` object.

```python
orders.schema
```

Useful when working with custom schemas or programmatically accessing metadata.

---

## inputFiles()

Returns the list of input files used to create the DataFrame.

```python
orders.inputFiles()
```

Useful for verifying the source of the data.

---

# Example Execution

```python
orders = spark.read.csv("orders.csv", header=True)

filtered = orders.filter("quantity > 2")

filtered.show()
```

Execution Flow:

```
Read CSV
    │
    ▼
Filter
    │
    ▼
show()
    │
    ▼
Job Created
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
Display Results
```

---

# Action vs Utility Method

| Action | Utility Method |
|---------|----------------|
| Triggers Spark execution | Does not represent a computation |
| Creates one or more Jobs | Used for inspection and debugging |
| Returns data or writes data | Displays metadata or execution information |
| Examples: `show()`, `count()`, `collect()`, `write()` | Examples: `printSchema()`, `explain()`, `columns`, `dtypes` |

---

# Best Practices

- Use `show()` to preview data.
- Use `take()` when only a few rows are needed.
- Use `count()` only when necessary because it scans the entire dataset.
- Avoid `collect()` on large datasets.
- Use `printSchema()` before writing transformations.
- Use `explain()` to understand Spark's execution plan and optimize queries.
- Use `write()` to save the final output.

---

# Key Takeaways

- Spark operations are grouped into **Transformations**, **Actions**, and **Utility Methods**.
- Transformations define **what** should happen.
- Actions trigger the execution of the computation.
- Utility methods help inspect the DataFrame and understand Spark's behavior.
- Every Action typically creates one or more Jobs.
- Jobs are divided into Stages, and Stages are divided into Tasks.
- Choosing the appropriate Action and using utility methods effectively can improve debugging and application performance.