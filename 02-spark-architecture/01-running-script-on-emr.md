# 01 — Running a Spark Script on Amazon EMR

## Overview

So far, we have worked with Spark locally and understood how Spark applications work internally.

The next step is to run the same PySpark application on a real distributed cluster.

Amazon EMR provides a managed environment for running frameworks such as:

* Apache Spark
* Hadoop
* Hive
* Presto/Trino

In this lesson, we will focus on running a **PySpark script on an EMR cluster**.

The overall workflow is:

```text
Write PySpark Script
        ↓
Upload Script to S3
        ↓
Create / Start EMR Cluster
        ↓
Submit Spark Job
        ↓
Spark Driver
        ↓
Executors
        ↓
Process Data
        ↓
Write Output
        ↓
Validate Results
        ↓
Inspect Logs / Spark UI
```

---

# 1. What Are We Trying to Do?

Suppose we have this PySpark application:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EMRExample") \
    .getOrCreate()

df = spark.read.parquet(
    "s3://my-bucket/input/orders/"
)

result = (
    df.groupBy("customer_id")
      .sum("revenue")
)

result.write.mode("overwrite").parquet(
    "s3://my-bucket/output/customer_revenue/"
)

spark.stop()
```

Locally, we might run:

```bash
spark-submit application.py
```

On EMR, the same application can run on a distributed cluster.

The major difference is:

```text
Local
    ↓
Single Machine

EMR
    ↓
Distributed Cluster
```

---

# 2. EMR Cluster Architecture

A typical EMR cluster contains:

```text
EMR Cluster
│
├── Primary Node
│   └── Spark Driver
│
└── Core / Task Nodes
    └── Spark Executors
```

The exact roles depend on the cluster configuration.

Conceptually:

```text
                 EMR Cluster
                     │
              ┌──────┴──────┐
              │             │
         Primary Node    Core/Task Nodes
              │             │
        Spark Driver    Spark Executors
              │             │
              └──────┬──────┘
                     │
                  S3 / Data
```

---

# 3. Primary Node

The primary node coordinates the cluster.

For a Spark application, the driver runs on the cluster's primary node when using the normal EMR cluster execution model.

The driver is responsible for:

* Creating the Spark application
* Creating the SparkSession
* Building the execution plan
* Scheduling jobs
* Coordinating tasks

It does not perform all distributed data processing itself.

---

# 4. Core Nodes

Core nodes provide compute capacity and also typically participate in distributed storage when Hadoop storage is used.

For Spark:

```text
Core Nodes
    ↓
Executor Capacity
```

For example:

```text
Primary Node
    │
    ├── Driver
    │
    ▼
Core Node 1
    └── Executor

Core Node 2
    └── Executor

Core Node 3
    └── Executor
```

---

# 5. Task Nodes

Task nodes can provide additional compute capacity.

They are useful when we want additional Spark execution capacity without relying on them for HDFS storage.

Conceptually:

```text
Core Nodes
    +
Task Nodes
    ↓
More Compute Capacity
```

The exact behavior depends on the EMR release and cluster configuration.

---

# 6. Where Does the Data Live?

A common EMR architecture is:

```text
S3
│
├── Input Data
├── Scripts
├── Output Data
└── Logs
```

For example:

```text
s3://my-bucket/
│
├── scripts/
│   └── customer_revenue.py
│
├── input/
│   └── orders/
│
└── output/
    └── customer_revenue/
```

This is useful because S3 provides durable object storage independent of the lifecycle of the EMR cluster.

---

# 7. Upload the PySpark Script to S3

Suppose our local file is:

```text
customer_revenue.py
```

Upload it using AWS CLI:

```bash
aws s3 cp customer_revenue.py \
    s3://my-bucket/scripts/customer_revenue.py
```

Verify:

```bash
aws s3 ls s3://my-bucket/scripts/
```

Expected:

```text
customer_revenue.py
```

---

# 8. Why Put the Script in S3?

The script can technically be placed on the cluster in other ways.

However, S3 is convenient because:

```text
Local Machine
      ↓
      S3
      ↓
EMR Cluster
      ↓
Spark Application
```

The script becomes accessible to the cluster and can also be versioned and managed as part of a data-engineering workflow.

---

# 9. Connect to the EMR Cluster

If SSH access is configured, we can connect to the primary node.

Example:

```bash
ssh -i my-key.pem hadoop@<primary-node>
```

After connecting:

```bash
hostname
```

You should see the hostname of the EMR primary node.

You can also check Spark:

```bash
spark-submit --version
```

---

# 10. Download the Script to the Primary Node

One approach is:

```bash
aws s3 cp \
    s3://my-bucket/scripts/customer_revenue.py \
    /home/hadoop/customer_revenue.py
```

Now:

```bash
ls -lh /home/hadoop/
```

You should see:

```text
customer_revenue.py
```

---

# 11. Submit the Spark Application

The most common approach is:

```bash
spark-submit customer_revenue.py
```

Spark then creates the application.

Conceptually:

```text
spark-submit
     │
     ▼
Spark Driver
     │
     ▼
Cluster Manager
     │
     ▼
Executors
     │
     ▼
Tasks
```

---

# 12. What Happens After `spark-submit`?

When we run:

```bash
spark-submit customer_revenue.py
```

the following happens conceptually:

```text
1. Script starts
       ↓
2. SparkSession created
       ↓
3. Spark application initialized
       ↓
4. Driver creates execution plan
       ↓
5. Resources requested
       ↓
6. Executors start
       ↓
7. Tasks are created
       ↓
8. Executors process partitions
       ↓
9. Results written
       ↓
10. Application finishes
```

---

# 13. SparkSession on EMR

Our script may contain:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CustomerRevenue") \
    .getOrCreate()
```

On EMR, Spark is already installed and configured.

We do not need to manually install Spark on every node.

EMR manages the underlying Spark environment.

---

# 14. Reading Data From S3

PySpark can read S3 data directly.

For example:

```python
df = spark.read.parquet(
    "s3://my-bucket/input/orders/"
)
```

or:

```python
df = spark.read.csv(
    "s3://my-bucket/input/orders.csv",
    header=True,
    inferSchema=True
)
```

Conceptually:

```text
S3
 │
 │ Read
 ▼
Spark Executors
 │
 ├── Partition 1
 ├── Partition 2
 ├── Partition 3
 └── Partition 4
```

The data is processed in distributed fashion.

---

# 15. Reading S3 Is Not the Same as Downloading the Whole File

A common misunderstanding is:

> "Spark downloads the complete dataset to the primary node."

Normally, Spark reads the data through the configured Hadoop/S3 filesystem integration and processes partitions across executors.

Conceptually:

```text
S3
 │
 ├── Data Partition
 ├── Data Partition
 ├── Data Partition
 └── Data Partition
       │
       ▼
Executors
```

The primary node does not need to hold the complete dataset.

---

# 16. Writing Output to S3

Example:

```python
result.write \
    .mode("overwrite") \
    .parquet(
        "s3://my-bucket/output/customer_revenue/"
    )
```

Spark generally writes distributed output.

The output may look like:

```text
customer_revenue/
│
├── part-00000-....
├── part-00001-....
├── part-00002-....
└── _SUCCESS
```

The number of part files depends on the number of output partitions/tasks.

---

# 17. Why Do We Get Multiple Part Files?

Suppose:

```text
Output Data
    ↓
4 Partitions
```

Spark can write:

```text
part-00000
part-00001
part-00002
part-00003
```

because multiple executors/tasks can write data in parallel.

This is a fundamental property of distributed processing.

---

# 18. Validate the Output

After the application finishes:

```bash
aws s3 ls \
    s3://my-bucket/output/customer_revenue/
```

You may see:

```text
_SUCCESS
part-00000-...
part-00001-...
```

You can also read the output using Spark:

```python
result = spark.read.parquet(
    "s3://my-bucket/output/customer_revenue/"
)

result.show()
```

---

# 19. Running the Script Directly From S3

We do not necessarily need to download the script first.

Spark can run a script stored in S3:

```bash
spark-submit \
    s3://my-bucket/scripts/customer_revenue.py
```

This gives us:

```text
S3
│
├── Script
├── Input
└── Output

       ↓

EMR
│
└── Spark Application
```

This is often cleaner for automated pipelines.

---

# 20. Passing Arguments to the Script

Suppose our script accepts command-line arguments.

```python
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]
```

Run:

```bash
spark-submit \
    s3://my-bucket/scripts/customer_revenue.py \
    s3://my-bucket/input/orders/ \
    s3://my-bucket/output/customer_revenue/
```

Now the same script can process different datasets or output locations.

---

# 21. Better Production Pattern

Instead of hardcoding:

```python
input_path = "s3://bucket/input/orders/"
```

use parameters:

```python
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]
```

Then:

```bash
spark-submit \
    s3://bucket/scripts/job.py \
    s3://bucket/input/ \
    s3://bucket/output/
```

This makes the Spark job reusable.

---

# 22. EMR Steps

Instead of manually SSHing into the cluster, EMR supports submitting work as **steps**.

Conceptually:

```text
EMR Cluster
    │
    ▼
EMR Step
    │
    ▼
Spark Application
```

A step can execute a command such as:

```bash
spark-submit \
    s3://my-bucket/scripts/customer_revenue.py
```

This is much more suitable for automated workflows.

---

# 23. AWS CLI Example: Add an EMR Step

A typical pattern is:

```bash
aws emr add-steps \
    --cluster-id j-XXXXXXXXXXXXX \
    --steps Type=Spark,Name="Customer Revenue",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://my-bucket/scripts/customer_revenue.py]
```

The exact command can vary depending on the EMR release and application configuration.

The important concept is:

```text
AWS CLI
   ↓
EMR Step
   ↓
Spark-submit
   ↓
Spark Application
```

---

# 24. Deploy Mode

Spark has two common deployment modes:

```text
client
cluster
```

### Client Mode

The driver runs where `spark-submit` was launched.

Conceptually:

```text
Client / Primary Node
        │
      Driver
        │
        ▼
    Executors
```

### Cluster Mode

The driver runs inside the cluster.

Conceptually:

```text
Cluster
│
├── Driver
│
└── Executors
```

For production-style distributed execution, cluster mode is commonly preferred because the driver is managed within the cluster environment.

---

# 25. Check Running Applications

On the EMR primary node, you can inspect processes and cluster services.

For Spark applications, the Spark UI is particularly useful.

Depending on the EMR configuration, you may access Spark UI through:

```text
Spark History Server
```

or EMR's application interfaces.

---

# 26. Spark UI

Spark UI helps us understand:

```text
Jobs
Stages
Tasks
Executors
SQL
Storage
Environment
```

For example:

```text
Spark Application
       │
       ├── Job 0
       │    ├── Stage 0
       │    └── Stage 1
       │
       └── Job 1
            └── Stage 2
```

This is extremely useful for troubleshooting.

---

# 27. Logs

When a Spark application fails, logs are one of the first things to inspect.

Look for:

```text
ERROR
Exception
OutOfMemoryError
AnalysisException
FileNotFoundException
AccessDenied
```

Examples:

```text
java.lang.OutOfMemoryError
```

or:

```text
org.apache.spark.sql.AnalysisException
```

or:

```text
AccessDeniedException
```

The exact error usually points toward the problem category.

---

# 28. Common EMR Problem: S3 Access

Suppose:

```python
df = spark.read.parquet(
    "s3://my-bucket/input/"
)
```

fails with an access error.

Possible cause:

```text
IAM permissions
```

The EMR runtime role needs appropriate permissions to access the S3 bucket and objects.

Conceptually:

```text
Spark
  ↓
S3 Request
  ↓
IAM Authorization
  ↓
Allow / Deny
```

This is why AWS permissions are part of running Spark on EMR.

---

# 29. Common Problem: Wrong S3 Path

Example:

```text
s3://my-bucket/input/orders/
```

but the actual location is:

```text
s3://my-bucket/data/orders/
```

Spark may return a path-related error.

Always verify:

```bash
aws s3 ls s3://my-bucket/
```

and:

```bash
aws s3 ls s3://my-bucket/input/
```

before troubleshooting Spark itself.

---

# 30. Common Problem: Script Not Found

If:

```bash
spark-submit s3://my-bucket/scripts/job.py
```

fails, verify:

```bash
aws s3 ls s3://my-bucket/scripts/
```

Make sure:

* Bucket is correct
* Prefix is correct
* File name is correct
* IAM permissions are correct

---

# 31. Common Problem: Cluster Terminates

EMR clusters can be configured with auto-termination.

For example:

```text
No active steps
      ↓
Idle period
      ↓
Cluster terminates
```

This is useful for controlling cost.

But it can be confusing during development if you expect the cluster to remain available indefinitely.

Always check the cluster state:

```bash
aws emr describe-cluster \
    --cluster-id j-XXXXXXXXXXXXX
```

---

# 32. Check EMR Cluster State

Useful command:

```bash
aws emr describe-cluster \
    --cluster-id j-XXXXXXXXXXXXX \
    --query 'Cluster.Status.State'
```

Possible states include:

```text
STARTING
BOOTSTRAPPING
RUNNING
WAITING
TERMINATING
TERMINATED
```

The exact lifecycle depends on the cluster configuration and EMR behavior.

---

# 33. Check EMR Steps

List steps:

```bash
aws emr list-steps \
    --cluster-id j-XXXXXXXXXXXXX
```

This helps answer:

```text
Did the job start?
Did it finish?
Did it fail?
What is the step state?
```

Typical step states include:

```text
PENDING
RUNNING
COMPLETED
CANCEL_PENDING
CANCELLED
FAILED
INTERRUPTED
```

---

# 34. A Complete Example

Suppose our project structure is:

```text
S3 Bucket
│
├── scripts/
│   └── customer_revenue.py
│
├── input/
│   └── orders/
│
└── output/
```

Upload the script:

```bash
aws s3 cp \
    customer_revenue.py \
    s3://my-bucket/scripts/customer_revenue.py
```

Verify:

```bash
aws s3 ls \
    s3://my-bucket/scripts/
```

Create or start the EMR cluster.

Then submit:

```bash
spark-submit \
    s3://my-bucket/scripts/customer_revenue.py
```

The application:

```text
Reads
  ↓
S3 Input
  ↓
Spark Processing
  ↓
Executors
  ↓
Aggregation
  ↓
S3 Output
```

Finally:

```bash
aws s3 ls \
    s3://my-bucket/output/
```

---

# 35. Production Workflow

A more production-oriented workflow looks like:

```text
Developer
    │
    ▼
Git Repository
    │
    ▼
CI/CD or Deployment Process
    │
    ▼
S3
    │
    ▼
EMR
    │
    ▼
EMR Step
    │
    ▼
Spark Application
    │
    ▼
S3 Output
    │
    ▼
Monitoring / Validation
```

The developer should not need to manually SSH into the cluster for every production job.

---

# 36. Local vs EMR Execution

### Local

```bash
spark-submit job.py
```

```text
Laptop
└── Spark
    └── Local Resources
```

### EMR

```bash
spark-submit job.py
```

```text
EMR Cluster
│
├── Driver
├── Executor
├── Executor
└── Executor
```

The application code can remain largely the same.

The major difference is the execution environment and available distributed resources.

---

# 37. What Changes When Moving From Local to EMR?

Usually, the core Spark transformations remain similar:

```python
df.filter(...)
df.groupBy(...)
df.join(...)
df.write.parquet(...)
```

But you must consider:

```text
S3 Paths
IAM Permissions
Cluster Resources
Deployment Mode
Executor Configuration
Network
Logging
Monitoring
Cost
```

---

# 38. Important Best Practices

### 1. Keep Input and Output in Durable Storage

Prefer:

```text
S3
```

for durable datasets.

---

### 2. Avoid Hardcoding Paths

Use arguments:

```python
input_path = sys.argv[1]
output_path = sys.argv[2]
```

---

### 3. Avoid `collect()` on Large Data

Keep processing distributed.

---

### 4. Use Parquet for Analytics Workloads

Parquet provides:

* Columnar storage
* Compression
* Predicate pushdown
* Efficient Spark processing

---

### 5. Monitor the Spark UI

Do not treat the job as a black box.

Inspect:

```text
Jobs
Stages
Tasks
Shuffle
Executors
SQL
```

---

### 6. Check Logs Before Changing Configuration

If the job fails:

```text
Error
 ↓
Understand Cause
 ↓
Fix
```

not:

```text
Error
 ↓
Increase Memory
```

---

# 39. End-to-End Mental Model

Keep this architecture in mind:

```text
                    Amazon EMR
                        │
              ┌─────────┴─────────┐
              │                   │
        Primary Node          Core/Task Nodes
              │                   │
           Driver             Executors
              │                   │
              └─────────┬─────────┘
                        │
                        ▼
                       S3
                ┌───────┼───────┐
                │       │       │
              Input   Script   Output
```

And the execution flow:

```text
PySpark Script
      ↓
spark-submit
      ↓
Driver
      ↓
Spark Plan
      ↓
Stages
      ↓
Tasks
      ↓
Executors
      ↓
Read / Transform / Shuffle
      ↓
Write Output
      ↓
S3
```

---

# 40. Key Takeaways

1. EMR provides a managed environment for running Spark on AWS.
2. The primary node coordinates the Spark application.
3. Executors perform distributed computation.
4. S3 is commonly used for durable input, output, scripts, and logs.
5. A PySpark script can be submitted using `spark-submit`.
6. A script can also be submitted directly from S3.
7. EMR Steps provide a better mechanism for automated job submission.
8. `spark-submit` starts the Spark application; the driver then coordinates execution.
9. Spark processes data through partitions and tasks across executors.
10. Output is usually written as multiple part files because Spark writes in parallel.
11. IAM permissions are essential when Spark accesses S3.
12. Spark UI and logs are critical for troubleshooting.
13. Auto-termination can terminate idle EMR clusters and helps control cost.
14. Production workflows should minimize manual SSH-based job execution.
15. Moving from local Spark to EMR changes the execution environment more than the Spark transformation logic.
16. The basic production pattern is:

```text
Code
 ↓
S3
 ↓
EMR
 ↓
EMR Step
 ↓
Spark
 ↓
S3 Output
 ↓
Monitoring
```

---

