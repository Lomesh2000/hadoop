# 🚀 NASA Web Server Log Analytics Pipeline

An end-to-end **Data Engineering and Analytics pipeline** for processing the original NASA HTTP web-server logs using **Python, Hadoop HDFS, Hadoop Streaming / MapReduce, YARN, SQL, automated data-quality validation, and a live Streamlit dashboard**.

The pipeline was validated on the original NASA trace containing **3,461,612 web requests** and produces daily traffic analytics, global Top-N requested resources, and hourly HTTP error-rate analysis.

---

## 🎥 Demo

> **▶ Click the preview below to watch the complete pipeline demo.**

[![NASA Log Analytics Demo](src/docs/demo-thumbnail.png)](YOUR_VIDEO_URL)

The demo shows:

- live pipeline execution
- Hadoop / YARN processing
- reducer configuration
- Q1, Q2 and Q3 results appearing after each job
- query execution timings
- data-quality validation
- final pipeline status
- analytical tables and charts

---

# 📌 Project Overview

The project implements a complete ETL and analytics workflow for NASA web-server logs.

```text
                       NASA Raw Web Logs
                              │
                              ▼
                  ┌──────────────────────┐
                  │ Raw Ingestion & Audit│
                  └──────────┬───────────┘
                             │
                             ▼
                           HDFS
                             │
                             ▼
                  Hadoop Streaming / YARN
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
             Q1             Q2             Q3
        Daily Traffic    Global Top-N   Hourly Errors
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                     SQLite / MySQL
                             │
                             ▼
                 Data Quality Gatekeeper
                             │
                             ▼
                  Reporting & Visualization
                             │
                             ▼
                     Streamlit Dashboard
```

---

# 🎯 What the Pipeline Does

The pipeline is organized into five stages.

## Stage 1 — Raw Log Ingestion & Audit

Reads the raw NASA log file, parses structured records, and keeps track of malformed records.

For the original dataset:

| Metric | Result |
|---|---:|
| Total records | **3,461,612** |
| Valid structured records | **3,461,580** |
| Malformed records | **32** |

The ingestion stage preserves an explicit accounting relationship:

```text
Total Input = Valid Records + Malformed Records
```

---

## Stage 2 — Distributed MapReduce Computation

The pipeline executes three analytical jobs using Python-based Hadoop Streaming.

### Q1 — Daily Traffic Analysis

Computes, per day:

- total request count
- total bytes transferred

The complete run produced **58 daily records**.

---

### Q2 — Global Top-N Requested Resources

Finds the most requested NASA resources.

The reducer maintains a **bounded min-heap**, keeping memory proportional to the requested `N` rather than buffering every resource.

For the full dataset, the Top-N output included:

| Rank | Resource | Requests |
|---:|---|---:|
| 1 | `/images/NASA-logosmall.gif` | 208,798 |
| 2 | `/images/KSC-logosmall.gif` | 164,976 |
| 3 | `/images/MOSAIC-logosmall.gif` | 127,916 |
| 4 | `/images/USA-logosmall.gif` | 127,082 |
| 5 | `/images/WORLD-logosmall.gif` | 125,933 |

### Why one reducer for Q2?

The current implementation performs a **single-stage global Top-N**.

With multiple reducers, each reducer would produce only a partition-local Top-N. A second aggregation stage would then be required to obtain the true global Top-N.

Therefore:

```text
Q2 → 1 reducer
```

is a deliberate correctness/design decision.

---

### Q3 — Hourly Error Analysis

Computes:

- total requests per hour
- HTTP error requests
- hourly error rate

An HTTP response with status code `>= 400` is treated as an error.

The full dataset produced **1,369 hourly records**.

Example high-error periods:

| Hour | Requests | Errors | Error Rate |
|---|---:|---:|---:|
| 1995-08-07 02:00 | 1,374 | 158 | 11.50% |
| 1995-08-06 02:00 | 1,066 | 100 | 9.38% |
| 1995-08-06 03:00 | 825 | 65 | 7.88% |

---

# ✅ Data Quality & Validation

A dedicated **Data Quality Gatekeeper** runs before downstream reporting.

The pipeline validates seven checks.

| Check | Purpose |
|---|---|
| DQ-01 | Raw ingestion accounting balance |
| DQ-02 | Cross-job request-count consistency |
| DQ-03 | Error-rate mathematical and bounds validation |
| DQ-04 | Daily traffic key / metric integrity |
| DQ-05 | Top-N ordering and cardinality |
| DQ-06 | Critical attribute non-nullability |
| DQ-07 | Malformed-record rate threshold |

For the complete NASA dataset:

```text
3,461,612 input records
3,461,580 valid
32 malformed

Overall Data Quality Verdict: PASSED
```

A failed validation gate stops downstream execution instead of allowing invalid analytics to reach the reporting layer.

---

# 🗄️ Database Layer

The analytical outputs are loaded into a relational database.

Supported modes include:

- SQLite
- MySQL

The loader uses:

- transactional writes
- parameterized SQL
- rollback on failure
- repeatable/idempotent loading behavior

The output tables are:

```text
daily_traffic
top_resources
hourly_errors
```

---

# 📊 Reporting & Visualization

The reporting layer generates:

- CSV analytical outputs
- daily traffic charts
- Top-N resource charts
- hourly error-rate visualizations
- summary tables

The Streamlit dashboard provides a live view of the entire execution.

### Dashboard Features

- execution mode
- reducer configuration
- total records
- valid / malformed records
- current pipeline stage
- live elapsed time
- Q1 table + chart
- Q2 table + chart
- Q3 table + chart
- data-quality status
- final pipeline timing
- full execution log

The dashboard displays a query's result **immediately after that MapReduce job finishes**, while the next job continues executing.

---

# ⚙️ Hadoop Execution

The project supports:

```text
LOCAL
HADOOP
```

### Local mode

Runs the MapReduce logic through local Unix/Python execution.

Useful for:

- development
- debugging
- fast iteration
- correctness checks

### Hadoop mode

Uses:

```text
HDFS
  ↓
YARN
  ↓
Hadoop Streaming
  ↓
Python Mapper
  ↓
Shuffle / Sort
  ↓
Python Reducer
  ↓
HDFS Output
```

The Hadoop mode was validated on a **single-node pseudo-distributed Hadoop 3.3.6 environment**.

---

# ⚡ Reducer Benchmark

Reducer count was experimentally varied on the complete **3.46M-record dataset**.

## Benchmark Results

| Configuration | Q1 Reducers | Q2 Reducer | Q3 Reducers | MapReduce Time | Full Pipeline Time |
|---|---:|---:|---:|---:|---:|
| 1 / 1 / 1 | 1 | 1 | 1 | **238.78 s** | **290.55 s** |
| 2 / 1 / 2 | 2 | 1 | 2 | **212.85 s** | **270.10 s** |
| 4 / 1 / 4 | 4 | 1 | 4 | **216.47 s** | **256.57 s** |

### Observation

Increasing reducers does **not** monotonically reduce runtime.

For example:

```text
1 reducer → 2 reducers
```

improved the MapReduce stage from:

```text
238.78s → 212.85s
```

which is approximately an **11% reduction**.

Increasing to four reducers then produced:

```text
216.47s
```

so the additional parallelism did not further improve the MapReduce stage.

The experiment demonstrates the trade-off between:

- reducer parallelism
- task/container startup overhead
- resource contention
- shuffle/coordination overhead

The exact optimum is environment- and workload-dependent.

> **Important:** these measurements were collected on a single-node Hadoop environment, so they demonstrate reducer/task tuning rather than multi-machine cluster scalability.

---

# 🔬 Local vs Hadoop

The same full dataset was also executed in local mode.

### Full 3.46M-record local run

```text
Input records:      3,461,612
Valid records:      3,461,580
Malformed:                 32
MapReduce stage:        94.02s
End-to-end:            145.24s
```

### Full 3.46M-record Hadoop run

One measured 4/1/4 execution:

```text
Input records:      3,461,612
Valid records:      3,461,580
Malformed:                 32
MapReduce stage:       216.47s
End-to-end:            256.57s
```

For this environment, local execution remained faster.

That is expected because Hadoop introduces distributed-system overhead such as:

- YARN application startup
- container startup
- HDFS I/O
- shuffle and sort
- task scheduling / coordination

The project therefore treats Hadoop as a **distributed execution architecture**, not as something that must always be faster than local execution.

---

# 🧠 Key Design Decisions

## Why Hadoop Streaming?

Python keeps the MapReduce implementation lightweight while allowing the same mapper/reducer logic to execute under Hadoop.

This also makes local development easier because the transformation logic can be tested without requiring a cluster.

---

## Why bounded heap for Top-N?

A naive reducer could store request counts for every unique URL.

The Top-N reducer instead maintains a bounded min-heap.

Memory complexity:

```text
O(N)
```

where `N` is the requested Top-N value.

This avoids retaining every candidate resource in memory.

---

## Why keep Q2 at one reducer?

The current Top-N implementation is a single-stage global aggregation.

Multiple reducers would produce partition-local candidates:

```text
Reducer 1 → local Top-N
Reducer 2 → local Top-N
...
```

A second global aggregation stage would be required to combine them into the true global Top-N.

---

## Why data-quality checks across independent jobs?

Q1 and Q3 independently calculate request totals.

The pipeline uses that independence to validate:

```text
Total Q1 Requests == Total Q3 Requests
```

This provides a cross-job invariant rather than validating each result in isolation.

---

# 🏗️ Project Structure

```text
.
├── README.md
├── src/
│   ├── config/
│   ├── data/
│   ├── database/
│   ├── docs/
│   ├── hive/
│   ├── mapreduce_python/
│   ├── orchestrator/
│   ├── pig/
│   ├── reporting/
│   ├── tests/
│   ├── validation/
│   ├── dashboard.py
│   └── requirements.txt
└── ...
```

---

# 🛠️ Tech Stack

### Data Engineering

- Python
- Hadoop HDFS
- Hadoop Streaming
- MapReduce
- YARN
- Hive
- Pig

### Database

- SQLite
- MySQL

### Analytics / Visualization

- Pandas
- Matplotlib
- Seaborn
- Plotly
- Streamlit

### Development

- Linux
- Git
- Git LFS
- Bash

---

# 🚀 Getting Started

## 1. Clone the repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd hadoop
```

---

## 2. Install Python dependencies

```bash
pip install -r src/requirements.txt
```

---

## 3. Configure Hadoop

The project was tested with:

```text
Hadoop 3.3.6
Java 11
```

Make sure:

```bash
which hadoop
which hdfs
which yarn
java -version
```

are working.

---

## 4. Start HDFS and YARN

For a pseudo-distributed local cluster:

```bash
start-dfs.sh
start-yarn.sh
```

Verify:

```bash
jps
```

Expected services include:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

Check HDFS:

```bash
hdfs dfsadmin -report
```

Check YARN:

```bash
yarn node -list
```

---

# ▶️ Running the Pipeline

## Local execution

```bash
python3 src/orchestrator/pipeline_orchestrator.py \
  --input src/data/original/NASA_access_log_full.log \
  --output-dir src \
  --mode local \
  --top-n 20
```

---

## Hadoop execution

```bash
python3 src/orchestrator/pipeline_orchestrator.py \
  --input src/data/original/NASA_access_log_full.log \
  --output-dir src \
  --mode hadoop \
  --top-n 20
```

---

# 🖥️ Launch the Dashboard

```bash
streamlit run src/dashboard.py
```

The dashboard lets you monitor:

```text
Pipeline
   ↓
Current Stage
   ↓
Elapsed Time
   ↓
Q1 Result + Chart
   ↓
Q2 Result + Chart
   ↓
Q3 Result + Chart
   ↓
Data Quality
   ↓
Final Timing
```

---

# 🧪 Testing

The project includes tests for:

- parser edge cases
- missing inputs
- malformed records
- data-quality failures
- benchmark execution

Run:

```bash
pytest src/tests/
```

---

# 📈 Reproducible Performance Experiment

To compare reducer configurations, change:

```yaml
hadoop:
  num_reducers_q1: 2
  num_reducers_q2: 1
  num_reducers_q3: 2
```

in:

```text
src/config/config.yaml
```

Then run:

```bash
python3 src/orchestrator/pipeline_orchestrator.py \
  --input src/data/original/NASA_access_log_full.log \
  --output-dir src \
  --mode hadoop \
  --top-n 20
```

The orchestrator reports:

- Q1 execution time
- Q2 execution time
- Q3 execution time
- MapReduce stage time
- final pipeline time

---

# ⚠️ Limitations

This project is designed as a practical Data Engineering / MapReduce demonstration rather than a production multi-node deployment.

Current limitations:

1. Hadoop benchmarking was performed on a **single-node pseudo-distributed environment**.
2. Q2 uses a single reducer because the current Top-N design is single-stage and globally aggregated.
3. Hadoop can be slower than local execution for workloads where distributed-system overhead dominates.
4. The current benchmark does not represent performance on a multi-node production cluster.
5. Large raw datasets are stored using Git LFS.

---

# 🔮 Possible Extensions

Potential next steps:

- multi-node Hadoop benchmarking
- two-stage distributed Global Top-N
- incremental log ingestion
- streaming ingestion with Kafka
- cloud deployment
- containerized Hadoop environment
- richer dashboard monitoring
- automatic benchmark comparison across reducer configurations
- partition-aware storage and retention

---

# 💡 What This Project Demonstrates

This project focuses on understanding the **engineering trade-offs** behind distributed data processing rather than simply applying Hadoop.

The implementation demonstrates:

- batch ingestion
- raw-data accounting
- distributed MapReduce execution
- HDFS storage
- YARN execution
- reducer tuning
- bounded-memory Top-N computation
- transactional persistence
- cross-job data validation
- failure containment
- performance benchmarking
- live pipeline observability

---

# 👨‍💻 Author

**Lomesh Soni**

M.Tech — AI & Data Science

---

# 📄 License

This project is intended for educational and portfolio use.
