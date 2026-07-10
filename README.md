# Serverless Cloud-Native Pipeline for Distributed Student Risk Analytics

A zero-infrastructure, event-driven data pipeline that ingests student
academic records, runs distributed processing with **Apache Spark
(PySpark)**, and automatically flags at-risk students — all without owning
or managing a single server.

---

## Problem statement

Colleges collect attendance and marks data every semester, but rarely turn
it into action until it's too late for the student. This project builds an
**automated, self-triggering pipeline** that:

1. Ingests student attendance + subject-wise marks
2. Runs distributed aggregation and rule-based risk detection
3. Publishes a branch-wise summary and a flagged at-risk list
4. Re-runs itself on a schedule, with zero standing infrastructure cost

---

## Architecture

```
 GitHub Actions scheduler (cron / manual trigger)
              |
              v
 Fresh isolated container spins up   <- ephemeral compute, zero idle cost
              |
              v
 Ingest student CSV data (Students + Marks)
              |
              v
 PySpark job: filter -> join -> groupBy -> orderBy
   (lazy DAG, executed only when an action - show()/write() - runs)
              |
              v
 Rule-based at-risk flag (Attendance < 75% AND AvgMarks < 40)
              |
              v
 Results committed back to the repository / logged as artifacts
```

This is a **serverless, event-driven architecture**: there is no
always-on server. Compute exists only for the ~1-2 minutes the pipeline
runs, inside a container GitHub provisions and destroys automatically.

---

## Why this maps to Cloud Computing concepts

| Concept from the course | Where it shows up here |
|---|---|
| **On-demand self-service** | Pipeline is triggered on-demand (or on schedule) with no manual server provisioning |
| **Measured / pay-per-use service** | GitHub Actions bills by the minute of container usage — nothing runs, nothing costs |
| **Virtualization / Containers** | Every run happens inside a fresh, isolated `ubuntu-latest` container |
| **Serverless (FaaS-style) computing** | No server is owned, patched, or kept running by us |
| **Lazy evaluation** | `filter`, `join`, `groupBy`, `orderBy` are all transformations — nothing executes until `.show()` / `.write()` (an action) is called |
| **Narrow vs wide transformations** | `filter` is narrow (no shuffle); `join`, `groupBy`, `orderBy` are wide (shuffle across partitions) |
| **Distributed processing** | Spark splits the DataFrame into partitions and processes them in parallel across available cores |
| **Cloud economics (zero standing cost)** | No VM is rented 24/7 — compute is billed only for actual run time |

---

## Repository structure

```
StudentRiskPipeline/
├── data/
│   ├── students.csv          # StudentID, Name, Branch, Semester, Attendance
│   └── marks.csv              # StudentID, Subject, Marks
├── scripts/
│   ├── generate_data.py       # creates the synthetic dataset
│   ├── spark_pipeline.py      # the main PySpark job
│   └── benchmark.py           # Pandas vs PySpark performance comparison
├── output/
│   ├── branch_summary_csv/    # branch-wise aggregated report
│   ├── full_report_csv/       # every student + at-risk flag
│   └── benchmark_results.csv  # timing comparison
├── .github/workflows/
│   └── student-risk-pipeline.yml   # the serverless scheduler
├── requirements.txt
└── README.md
```

---

## How to run it locally

```bash
pip install -r requirements.txt
python scripts/generate_data.py     # creates data/students.csv and data/marks.csv
python scripts/spark_pipeline.py    # runs the full Spark pipeline
python scripts/benchmark.py         # optional: Pandas vs PySpark timing comparison
```

## How to run it "in the cloud" (GitHub Actions)

1. Push this repository to GitHub.
2. Go to the **Actions** tab.
3. Select **Student Risk Analytics Pipeline** -> **Run workflow** (manual trigger),
   or simply wait for the daily 03:00 UTC scheduled run.
4. The workflow provisions a fresh container, runs the pipeline, and commits
   the updated `output/` files back to the repo automatically.

---

## Sample results (500 synthetic students)

**Branch-wise summary**

| Branch | Total Students | Avg Attendance | Avg Marks | At-Risk Count |
|---|---|---|---|---|
| ECE | 89 | 72.28% | 47.59 | 11 |
| Mechanical | 108 | 73.12% | 47.61 | 15 |
| AI | 96 | 72.24% | 47.97 | 15 |
| CSE | 100 | 72.21% | 48.05 | 14 |
| Cloud | 107 | 71.23% | 48.09 | 12 |

**Overall: 67 / 500 students (13.4%) flagged at-risk** —
Attendance < 75% AND Average Marks < 40.

---

## Pandas vs PySpark benchmark

The pipeline also benchmarks Pandas (single-threaded) against PySpark
(distributed) on the same aggregation task at increasing data sizes. On a
single-core machine, PySpark's JVM startup, task-scheduling, and shuffle
overhead outweighs Pandas' simplicity — Pandas wins locally. PySpark's
advantage appears once the workload runs across **multiple cores or
multiple machines in a real cluster**, or once data grows too large to
fit in one machine's memory. This tradeoff — "distributed computing adds
overhead that only pays off at scale" — is itself one of the course's
core lessons.

---

## Tech stack

- **Apache Spark (PySpark)** — distributed data processing
- **Python 3.11**
- **GitHub Actions** — serverless scheduler / compute
- **Pandas** — baseline comparison

---

## Author

Yuvraj — B.S. Data Science and Engineering, IISER Bhopal
