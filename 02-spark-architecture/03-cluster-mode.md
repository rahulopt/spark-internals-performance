# 03 — Spark Cluster Mode

## Overview

Spark applications can run in different deployment modes.

The two important modes are:

```text
Client Mode
Cluster Mode
```

The biggest difference is:

> **Where does the Spark Driver run?**

This distinction is extremely important for understanding Spark on EMR, YARN, Kubernetes, and other cluster environments.

---

# 1. What Is Deploy Mode?

When we submit a Spark application, Spark needs to determine where the **Driver** should run.

The two common choices are:

```text
--deploy-mode client
--deploy-mode cluster
```

Conceptually:

```text
Client Mode
    ↓
Driver runs where spark-submit is launched

Cluster Mode
    ↓
Driver runs inside the cluster
```

---

# 2. Client Mode

In client mode, the Spark Driver runs in the environment from which `spark-submit` was executed.

For example:

```bash
spark-submit \
    --deploy-mode client \
    application.py
```

If `spark-submit` is executed from a machine:

```text
Your Machine
    │
    └── Driver
```

then the driver runs on that machine.

The executors run on the cluster.

---

# 3. Client Mode Architecture

```text
                 Client Machine
                       │
                 Spark Driver
                       │
                       │
              Cluster Manager
                       │
          ┌────────────┼────────────┐
          │            │            │
      Executor      Executor     Executor
          │            │            │
        Tasks        Tasks        Tasks
          │            │            │
      Partitions   Partitions   Partitions
```

The important point:

```text
Driver → Outside the cluster
Executors → Inside the cluster
```

---

# 4. Cluster Mode

In cluster mode, the Spark Driver runs inside the cluster.

For example:

```bash
spark-submit \
    --deploy-mode cluster \
    application.py
```

The machine submitting the application does not host the driver.

Instead:

```text
Client
  │
  │ Submit
  ▼
Cluster Manager
  │
  ▼
Cluster
  │
  ├── Driver
  │
  ├── Executor
  ├── Executor
  └── Executor
```

This is why cluster mode is commonly preferred for production workloads.

---

# 5. Cluster Mode Architecture

```text
                    Client
                      │
                 spark-submit
                      │
                      ▼
               Cluster Manager
                      │
          ┌───────────┴───────────┐
          │                       │
       Driver                 Executors
          │                  ┌────┼────┐
          │                  │    │    │
          ▼                  ▼    ▼    ▼
       Scheduler           Tasks Tasks Tasks
                               │
                          Partitions
```

The key difference:

```text
Driver → Inside cluster
Executors → Inside cluster
```

---

# 6. Client vs Cluster Mode

| Feature                    | Client Mode        | Cluster Mode      |
| -------------------------- | ------------------ | ----------------- |
| Driver location            | Submission machine | Inside cluster    |
| Executors                  | Cluster            | Cluster           |
| Driver network dependency  | Higher             | Lower             |
| Suitable for development   | Yes                | Yes               |
| Suitable for production    | Sometimes          | Usually preferred |
| Client must stay connected | Often important    | Not necessarily   |
| Driver failure affects app | Yes                | Yes               |
| Driver resources           | Client machine     | Cluster resources |

The most important row is:

```text
Driver Location
```

---

# 7. Simple Example

Imagine:

```text
Laptop
   │
   │ spark-submit
   ▼
EMR Cluster
```

### Client Mode

```text
Laptop
│
└── Driver
      │
      ▼
EMR
├── Executor
├── Executor
└── Executor
```

### Cluster Mode

```text
Laptop
│
└── spark-submit
      │
      ▼
EMR
├── Driver
├── Executor
├── Executor
└── Executor
```

So the major difference is where the Driver lives.

---

# 8. Why Does Driver Location Matter?

The Driver communicates with executors.

In client mode:

```text
Driver
  │
  │ Network
  ▼
Executors
```

If the driver is running outside the cluster, network connectivity between the driver and executors becomes important.

In cluster mode:

```text
Cluster
│
├── Driver
│
└── Executors
```

The driver is inside the cluster environment.

This generally makes production deployment more robust.

---

# 9. Client Mode Example on EMR

Suppose you SSH into the EMR primary node:

```bash
ssh -i my-key.pem hadoop@<primary-node>
```

Then run:

```bash
spark-submit \
    --deploy-mode client \
    s3://my-bucket/scripts/job.py
```

The driver runs on the EMR primary node because that is where `spark-submit` was executed.

```text
EMR Primary Node
│
├── Driver
│
└── spark-submit
```

Executors run on the worker nodes.

```text
Worker Node 1 → Executor
Worker Node 2 → Executor
Worker Node 3 → Executor
```

---

# 10. Cluster Mode Example

Now:

```bash
spark-submit \
    --deploy-mode cluster \
    s3://my-bucket/scripts/job.py
```

The submission request goes to the cluster manager.

The cluster manager launches the Driver inside the cluster.

```text
Client
  │
  │ Submit
  ▼
Cluster Manager
  │
  ▼
Driver
  │
  ├── Executor
  ├── Executor
  └── Executor
```

The client does not need to host the Driver.

---

# 11. What Happens to the Terminal?

This is an important practical difference.

In client mode, if the Driver is running in your terminal/session:

```text
Terminal
   │
   └── Driver
```

closing that environment can terminate the application.

For example:

```text
SSH Session
    ↓
Driver
    ↓
Application
```

If the SSH session disappears and the driver process is terminated, the application can fail.

This is one reason client mode is less convenient for long-running production jobs when launched manually.

---

# 12. Cluster Mode and SSH

With cluster mode:

```text
SSH
 │
 └── Submit application
          │
          ▼
      Cluster
          │
       Driver
```

Once the application has been accepted and launched by the cluster manager, the Driver runs inside the cluster.

Therefore, the submitting client does not need to continuously host the Driver.

---

# 13. Important Clarification

Cluster mode does **not** mean:

> "The Spark application runs only on one cluster node."

It means:

> **The Driver runs inside the cluster.**

Executors still distribute the actual data processing across available worker resources.

```text
Cluster
│
├── Driver
│
├── Executor
├── Executor
└── Executor
```

---

# 14. Cluster Mode Does Not Mean More Parallelism

Changing:

```text
client
```

to:

```text
cluster
```

does not automatically increase:

* Number of executors
* Number of cores
* Number of partitions
* Number of tasks
* Data processing speed

Deploy mode primarily determines:

```text
Driver Location
```

Resource configuration determines:

```text
How much compute is available
```

---

# 15. Driver Resources

The Driver also requires:

```text
CPU
Memory
```

For example, the Driver may need more memory if the application creates large driver-side objects.

Bad example:

```python
large_data = df.collect()
```

This brings data to the Driver.

If the result is huge:

```text
Executors
    ↓
Large Data
    ↓
Driver
    ↓
OutOfMemoryError
```

Changing deployment mode does not automatically solve poor driver-side operations.

---

# 16. `collect()` and Driver

Consider:

```python
data = df.collect()
```

`collect()` brings all rows to the Driver.

Architecture:

```text
Executor 1 ─┐
Executor 2 ─┼──► Driver
Executor 3 ─┘
```

If the dataset is large, the Driver can run out of memory.

This is why we should avoid:

```python
df.collect()
```

for large datasets.

---

# 17. `show()` vs `collect()`

This is an important distinction.

```python
df.show()
```

is designed to display a limited number of rows.

Whereas:

```python
df.collect()
```

returns all rows to the Driver.

Therefore:

```text
show()
→ Limited display

collect()
→ Entire result to Driver
```

---

# 18. Client Mode Use Cases

Client mode can be useful for:

* Development
* Interactive debugging
* Spark shell
* Jupyter-based work
* Testing
* Interactive workloads

For example:

```text
Developer
   │
   ▼
EMR Primary Node
   │
 Driver
   │
 Executors
```

You can observe the application interactively.

---

# 19. Cluster Mode Use Cases

Cluster mode is generally more suitable for:

* Production jobs
* Scheduled jobs
* Automated pipelines
* Long-running batch applications
* Jobs submitted through orchestration systems

Conceptually:

```text
Scheduler
    │
    ▼
Cluster Manager
    │
    ▼
Spark Application
    │
    ├── Driver
    └── Executors
```

The submitting system does not need to host the Driver.

---

# 20. `spark-submit` Syntax

Basic syntax:

```bash
spark-submit \
    --master <master-url> \
    --deploy-mode <client|cluster> \
    application.py
```

For example:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    application.py
```

Or:

```bash
spark-submit \
    --master yarn \
    --deploy-mode client \
    application.py
```

The exact `--master` value depends on the cluster manager.

---

# 21. `--master` vs `--deploy-mode`

These two options are often confused.

### `--master`

Defines:

> **Which cluster manager / execution environment should Spark use?**

Examples:

```text
local[*]
yarn
k8s://...
spark://...
```

### `--deploy-mode`

Defines:

> **Where should the Driver run?**

Usually:

```text
client
cluster
```

So:

```text
--master
    ↓
Where / which resource manager?

--deploy-mode
    ↓
Where does Driver run?
```

---

# 22. Example

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    application.py
```

Interpretation:

```text
--master yarn
    ↓
Use YARN for resource management

--deploy-mode cluster
    ↓
Run Driver inside the cluster
```

---

# 23. EMR Context

On Amazon EMR, Spark runs within the EMR-managed cluster environment.

A common architecture is:

```text
                    EMR Cluster
                         │
              ┌──────────┴──────────┐
              │                     │
        Primary Node           Core / Task Nodes
              │                     │
           Driver               Executors
              │                     │
              └──────────┬──────────┘
                         │
                         ▼
                        S3
```

When submitting Spark applications on EMR, you should understand whether the application is being launched interactively from the cluster or submitted as an automated EMR step.

---

# 24. EMR Steps

A production-style EMR workflow commonly uses EMR Steps.

Conceptually:

```text
AWS / Scheduler
      │
      ▼
EMR Step
      │
      ▼
Spark Application
      │
      ├── Driver
      │
      └── Executors
```

This avoids making an SSH session the central part of the production execution process.

---

# 25. Client Mode Mental Model

Remember:

```text
Client Mode

Submission Machine
       │
     Driver
       │
       ▼
    Cluster
    ├── Executor
    ├── Executor
    └── Executor
```

Driver is outside the worker execution environment.

---

# 26. Cluster Mode Mental Model

Remember:

```text
Cluster Mode

Submission Machine
       │
       │ Submit
       ▼
    Cluster
    ├── Driver
    ├── Executor
    ├── Executor
    └── Executor
```

Driver is inside the cluster.

---

# 27. Failure Scenario — Client Mode

Suppose:

```text
Laptop
  │
 Driver
  │
 Executors
```

Your laptop loses network connectivity.

Potential result:

```text
Driver unavailable
       ↓
Executors lose coordinator
       ↓
Application may fail
```

The exact behavior depends on the deployment environment and configuration, but the fundamental issue is that the Driver is tied to the client environment.

---

# 28. Failure Scenario — Cluster Mode

Now:

```text
Laptop
   │
Submit
   │
   ▼
Cluster
├── Driver
└── Executors
```

After submission, the Driver is managed inside the cluster environment.

If the laptop disconnects:

```text
Laptop
   X
   │
   │
Cluster
├── Driver
└── Executors
```

the Spark application can continue running, assuming the cluster and application itself remain healthy.

This is one of the major advantages of cluster mode.

---

# 29. Why Cluster Mode Is Better for Production

Production jobs should ideally not depend on:

```text
Developer Laptop
SSH Session
Open Terminal
Interactive Shell
```

Instead:

```text
Scheduler
    ↓
Cluster
    ↓
Driver
    ↓
Executors
```

This improves operational reliability and makes automation easier.

---

# 30. Client Mode Is Not Bad

Do not think:

```text
Client = Bad
Cluster = Good
```

That is incorrect.

Instead:

```text
Client Mode
→ Great for interactive development/debugging

Cluster Mode
→ Better suited for automated production workloads
```

The right choice depends on the workload and environment.

---

# 31. Common Interview Question

### Q: What is the difference between client and cluster mode?

A good answer:

> In client mode, the Spark Driver runs on the machine from which the application is submitted, while the executors run in the cluster. In cluster mode, the Driver itself runs inside the cluster along with the executors. Client mode is useful for interactive development and debugging, while cluster mode is generally preferred for production and automated workloads because the application does not depend on the submitting client to host the Driver.

---

# 32. Common Interview Question

### Q: Does cluster mode mean all Spark processing happens on one machine?

No.

Cluster mode only determines the Driver's location.

The actual processing remains distributed:

```text
Driver
  ↓
Executors
  ↓
Tasks
  ↓
Partitions
```

---

# 33. Common Interview Question

### Q: What is the difference between `--master` and `--deploy-mode`?

`--master` specifies the cluster manager or execution environment.

Example:

```bash
--master yarn
```

`--deploy-mode` specifies where the Driver runs.

Example:

```bash
--deploy-mode cluster
```

Therefore:

```text
master
→ Resource management environment

deploy-mode
→ Driver location
```

---

# 34. Common Interview Question

### Q: Why is cluster mode preferred for production?

Because the Driver runs inside the cluster rather than depending on the submitting machine.

This makes the application better suited for:

* Automated execution
* Scheduled jobs
* Long-running applications
* Production pipelines
* Remote submission

---

# 35. Common Interview Question

### Q: If I close my terminal, will a Spark job stop?

It depends on how the application was submitted.

If the Driver is running in the client environment, terminating that environment can terminate the Driver and therefore the application.

With cluster mode, the Driver runs inside the cluster, so the application can continue independently of the submitting client, assuming no other failure occurs.

---

# 36. Client vs Cluster — Visual Comparison

```text
CLIENT MODE
────────────────────────────────────

Client Machine
│
└── Driver
      │
      ▼
   Cluster
   ├── Executor
   ├── Executor
   └── Executor
```

```text
CLUSTER MODE
────────────────────────────────────

Client Machine
│
└── Submit
      │
      ▼
   Cluster
   ├── Driver
   ├── Executor
   ├── Executor
   └── Executor
```

---

# 37. Complete Spark Submission Flow

```text
                    spark-submit
                         │
                         ▼
                  Cluster Manager
                         │
                ┌────────┴────────┐
                │                 │
           Client Mode       Cluster Mode
                │                 │
                ▼                 ▼
          Driver on Client   Driver in Cluster
                │                 │
                └────────┬────────┘
                         │
                         ▼
                     Executors
                         │
                         ▼
                       Tasks
                         │
                         ▼
                    Partitions
```

---

# 38. Important Mental Model

Memorize this:

```text
--master
    ↓
Where does Spark get resources?

--deploy-mode
    ↓
Where does the Driver run?
```

And:

```text
Client Mode
    ↓
Driver outside cluster

Cluster Mode
    ↓
Driver inside cluster
```

---

# 39. Key Takeaways

1. Spark supports **client** and **cluster** deploy modes.
2. The main difference is the **Driver's location**.
3. In client mode, the Driver runs where `spark-submit` is executed.
4. In cluster mode, the Driver runs inside the cluster.
5. Executors run in the cluster in both modes.
6. `--master` specifies the resource-management environment.
7. `--deploy-mode` specifies the Driver's deployment location.
8. Client mode is useful for interactive development and debugging.
9. Cluster mode is generally preferred for production workloads.
10. Cluster mode does not mean processing happens on a single machine.
11. Executors still process partitions in parallel.
12. A Driver failure can affect the Spark application in either mode.
13. Client mode can create stronger dependency on the submitting environment.
14. Cluster mode allows the application to continue independently of the submitting client after successful submission.
15. Deploy mode does not automatically increase Spark parallelism.
16. More executors, cores, or memory must be configured separately.
17. `collect()` can cause Driver memory problems regardless of deploy mode.
18. Understanding Driver location is the easiest way to remember client vs cluster mode.

---

# Spark Architecture Series

```text
01 — running-script-on-emr.md
    Running Spark Script on EMR

02 — spark-architecture.md
    Spark Architecture

03 — cluster-mode.md
    Client Mode vs Cluster Mode

Next:
04 — ...
    Spark Configuration & Resource Management
```

> **Client mode: Driver runs on the submission machine. Cluster mode: Driver runs inside the cluster.**
