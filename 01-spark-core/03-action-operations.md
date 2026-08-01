# Utility Methods

Not every DataFrame operation in Spark is a Transformation or an Action.

Spark also provides several **utility methods** that help inspect or understand a DataFrame without performing distributed computation.

These methods are mainly used for debugging, exploration, and development.

| Method | Description | Transformation | Action |
|---------|-------------|:--------------:|:------:|
| `printSchema()` | Displays the DataFrame schema | ❌ | ❌ |
| `explain()` | Displays the logical and physical execution plan | ❌ | ❌ |
| `inputFiles()` | Returns the list of input files used by the DataFrame | ❌ | ❌ |
| `isEmpty()` | Checks whether a DataFrame is empty *(may trigger computation internally)* | ❌ | ❌* |
| `schema` | Returns the schema object | ❌ | ❌ |
| `columns` | Returns a list of column names | ❌ | ❌ |
| `dtypes` | Returns column names with their data types | ❌ | ❌ |

> **Note:** `isEmpty()` may internally execute a minimal job to determine if the DataFrame contains data, but it is generally considered a convenience or utility method rather than a standard Action like `count()` or `show()`.

---

## printSchema()

Displays the schema of a DataFrame.

```python
orders.printSchema()
```

Output:

```text
root
 |-- order_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
```

`printSchema()` helps you verify column names and data types before performing transformations.

---

## explain()

Displays Spark's execution plan.

```python
orders.filter("quantity > 2").explain()
```

Output (simplified):

```text
== Physical Plan ==
*(1) Filter (quantity > 2)
+- FileScan csv ...
```

This is useful for understanding how Spark plans to execute your query.

---

## columns

Returns all column names.

```python
orders.columns
```

Output:

```python
['order_id', 'customer_id', 'quantity', 'unit_price']
```

---

## dtypes

Returns each column along with its data type.

```python
orders.dtypes
```

Output:

```python
[
 ('order_id', 'string'),
 ('quantity', 'int'),
 ('unit_price', 'double')
]
```