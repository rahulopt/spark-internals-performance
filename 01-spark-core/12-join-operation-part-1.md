# 12. Join Operations — Part 1

Joins are used to combine data from two or more DataFrames based on a common column or condition.

In Spark, joins are important because data is distributed across **partitions and executors**.

---

## 📌 What is a Join?

Suppose we have two DataFrames:

### Customers

| customer_id | name  |
| ----------- | ----- |
| 101         | Rahul |
| 102         | Amit  |
| 103         | Priya |

### Orders

| order_id | customer_id | amount |
| -------- | ----------- | -----: |
| 1        | 101         |    500 |
| 2        | 102         |    800 |
| 3        | 101         |    300 |

To combine customer information with orders, we can join them using `customer_id`.

```python
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "inner"
)
```

---

## 🔹 Basic Join Syntax

```python
left_df.join(
    right_df,
    join_condition,
    join_type
)
```

Example:

```python
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "inner"
)
```

### Components

```text
left_df          → Left DataFrame
right_df         → Right DataFrame
join_condition   → Condition used to match rows
join_type        → Type of join
```

---

## 🔹 Join Using a Common Column

If both DataFrames contain the same column name:

```python
orders_df.join(
    customers_df,
    "customer_id",
    "inner"
)
```

Instead of:

```python
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "inner"
)
```

Using the column name is simpler when the join key is identical in both DataFrames.

---

# 🔹 Inner Join

An **INNER JOIN** returns only rows that have matching values in both DataFrames.

### Customers

```text
customer_id    name
-----------    -----
101            Rahul
102            Amit
103            Priya
```

### Orders

```text
order_id    customer_id    amount
--------    -----------    ------
1           101            500
2           102            800
3           104            200
```

### Join

```python
customers_df.join(
    orders_df,
    customers_df.customer_id == orders_df.customer_id,
    "inner"
)
```

### Result

```text
customer_id    name      order_id    amount
-----------    ----      --------    ------
101            Rahul     1           500
102            Amit      2           800
```

`103` is removed because there is no matching order.

`104` is removed because there is no matching customer.

### Mental Model

```text
INNER JOIN
     ↓
Matching rows only
     ↓
LEFT ∩ RIGHT
```

---

## 🔹 Join Condition

The join condition tells Spark **how rows should be matched**.

```python
orders_df.customer_id == customers_df.customer_id
```

The column names do not have to be the same.

```python
orders_df.customer_id == customers_df.id
```

This is also valid.

The important thing is the **condition**, not the column name.

---

## 🔹 Multiple Join Conditions

A join can contain multiple conditions.

```python
orders_df.join(
    customers_df,
    (orders_df.customer_id == customers_df.customer_id) &
    (orders_df.region == customers_df.region),
    "inner"
)
```

Both conditions must be satisfied:

```text
customer_id matches
        AND
region matches
```

---

# 🔹 Joins Are Lazy

A join is a **transformation**.

```python
joined_df = orders_df.join(
    customers_df,
    "customer_id"
)
```

Spark does not immediately execute the join.

Instead, Spark builds an execution plan:

```text
join()
   ↓
Logical Plan
   ↓
Catalyst Optimizer
   ↓
Physical Plan
   ↓
Execution
```

The actual execution happens when an **action** is triggered:

```python
joined_df.show()
```

or:

```python
joined_df.count()
```

---

# 🔹 Why Joins Are Different in Spark

Spark is a **distributed processing engine**.

Data is divided into partitions:

```text
Orders

Partition 0 → 101, 105
Partition 1 → 102, 103
Partition 2 → 104, 108
```

Another DataFrame may have:

```text
Customers

Partition 0 → 102, 104
Partition 1 → 101, 108
Partition 2 → 103, 105
```

Notice:

```text
Orders:
101 → Partition 0

Customers:
101 → Partition 1
```

The matching records are located in different partitions.

Spark therefore needs a way to bring matching keys together.

---

# 🔹 Join and Shuffle

A join may require data to be redistributed across partitions.

Conceptually:

```text
Before Join

Partition 0 → 101, 105
Partition 1 → 102, 103
Partition 2 → 104, 108

             ↓
          Shuffle
             ↓

Matching keys are brought
to appropriate partitions

             ↓
            JOIN
```

Shuffle can be expensive because it may involve:

* Network I/O
* Serialization
* CPU
* Disk I/O

Therefore, joins can become expensive when working with large datasets.

---

# 🔹 Join Strategies

Spark can use different physical strategies to execute joins.

Some important strategies are:

```text
Broadcast Hash Join
Sort Merge Join
Shuffled Hash Join
Broadcast Nested Loop Join
```

The strategy selected by Spark depends on factors such as:

* Data size
* Statistics
* Join condition
* Spark configuration
* Available memory

These strategies will be covered in **Part 2**.

---

# 🔹 Small Table + Large Table

Suppose:

```text
Orders     → 500 GB
Customers  → 20 MB
```

Moving the entire 500 GB dataset across the cluster can be expensive.

If `customers` is small enough, Spark can **broadcast** it to executors.

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
           Orders    Orders   Orders
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
   Executors
       ↓
Join with Large DataFrame
```

This can avoid a large shuffle.

---

# 🔹 Duplicate Column Names

Suppose both DataFrames contain:

```text
customer_id
```

Using an explicit join condition:

```python
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id
)
```

can result in duplicate column names.

Using aliases makes complex joins easier:

```python
orders = orders_df.alias("orders")
customers = customers_df.alias("customers")

joined_df = orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "inner"
)
```

Then:

```python
joined_df.select(
    "orders.customer_id",
    "orders.order_id",
    "customers.name"
)
```

---

# 🔹 SQL vs DataFrame API

### Spark SQL

```sql
SELECT *
FROM orders
INNER JOIN customers
ON orders.customer_id = customers.customer_id;
```

### DataFrame API

```python
orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "inner"
)
```

Both are optimized through Spark's SQL execution engine.

---

# 🧠 Key Takeaways

* **Join** combines data from two DataFrames based on a condition.
* **Inner Join** returns only matching rows.
* The **join condition** tells Spark how rows should be matched.
* A join is a **transformation**, so it follows Spark's lazy evaluation model.
* Spark data is distributed across **partitions**.
* Matching keys may exist on different partitions.
* Spark may need to **shuffle data** to bring matching keys together.
* Shuffle can be expensive because of **network, CPU, serialization, and disk I/O**.
* Spark has multiple **physical join strategies**.
* A small DataFrame can sometimes be **broadcast** to executors.
* Understanding joins requires thinking about **where the data is located**, not only the SQL syntax.

---

## ⭐ Most Important Question

Whenever you see:

```python
df1.join(df2, "id")
```

Don't think only:

> **"Two DataFrames are being combined."**

Think:

> **"How will Spark bring matching keys together across distributed partitions?"**

This leads to:

```text
Join
 ↓
Partitions
 ↓
Shuffle
 ↓
Join Strategy
 ↓
Performance
```

---

## ➡️ Next

**13. Join Operations — Part 2**

In Part 2:

* Broadcast Hash Join
* Sort Merge Join
* Shuffled Hash Join
* When shuffle happens
* How Spark chooses a join strategy
* `explain()` for joins
* Join performance optimization
