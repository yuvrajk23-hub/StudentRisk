"""
spark_pipeline.py
-------------------
The core distributed-processing job. Demonstrates the full PySpark pattern
taught in the course: lazy transformations (filter, join, groupBy, orderBy)
followed by an action (write/show) that triggers the DAG.

Stages:
  1. Load       -> spark.read.csv() creates two DataFrames (lazy)
  2. Clean      -> drop nulls / bad rows                      (narrow)
  3. Join       -> combine Students + Marks on StudentID       (wide, shuffle)
  4. Aggregate  -> per-student average marks (groupBy)         (wide, shuffle)
  5. Flag       -> rule-based "at-risk" detection               (narrow)
  6. Report     -> branch-wise summary (groupBy + orderBy)      (wide, shuffle)
  7. Action     -> .show() / .write.csv() triggers execution
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, when, col, round as spark_round

ATTENDANCE_THRESHOLD = 75.0
MARKS_THRESHOLD = 40.0


def build_spark_session():
    return (
        SparkSession.builder
        .appName("StudentRiskPipeline")
        .master("local[*]")   # runs on all locally available cores -> simulates a cluster
        .getOrCreate()
    )


def run_pipeline(spark, students_path, marks_path, output_dir):
    t0 = time.time()

    # ---- 1. LOAD (lazy) ----
    students_df = spark.read.csv(students_path, header=True, inferSchema=True)
    marks_df = spark.read.csv(marks_path, header=True, inferSchema=True)

    # ---- 2. CLEAN (narrow transformation) ----
    students_clean = students_df.dropna(subset=["StudentID", "Attendance"])
    marks_clean = marks_df.dropna(subset=["StudentID", "Marks"])

    # ---- 3. Per-student average marks (groupBy -> WIDE, shuffle) ----
    student_avg_marks = marks_clean.groupBy("StudentID").agg(
        spark_round(avg("Marks"), 2).alias("AvgMarks"),
        count("Subject").alias("SubjectsTaken"),
    )

    # ---- 4. JOIN Students with their average marks (WIDE, shuffle) ----
    merged = students_clean.join(student_avg_marks, on="StudentID", how="inner")

    # ---- 5. FLAG at-risk students (narrow transformation) ----
    flagged = merged.withColumn(
        "AtRisk",
        when(
            (col("Attendance") < ATTENDANCE_THRESHOLD) & (col("AvgMarks") < MARKS_THRESHOLD),
            "YES",
        ).otherwise("NO"),
    )

    # ---- 6. Branch-wise summary report (groupBy + orderBy -> WIDE) ----
    branch_summary = (
        flagged.groupBy("Branch")
        .agg(
            count("StudentID").alias("TotalStudents"),
            spark_round(avg("Attendance"), 2).alias("AvgAttendance"),
            spark_round(avg("AvgMarks"), 2).alias("AvgMarks"),
            count(when(col("AtRisk") == "YES", True)).alias("AtRiskCount"),
        )
        .orderBy(col("AvgMarks").asc())
    )

    at_risk_students = flagged.filter(col("AtRisk") == "YES").orderBy(
        col("Attendance").asc()
    )

    # ---- 7. ACTIONS (trigger execution) ----
    print("\n=== Branch-wise Summary ===")
    branch_summary.show(truncate=False)

    print("\n=== Sample At-Risk Students (lowest attendance first) ===")
    at_risk_students.select(
        "StudentID", "Name", "Branch", "Attendance", "AvgMarks", "AtRisk"
    ).show(15, truncate=False)

    total_students = flagged.count()
    total_at_risk = at_risk_students.count()

    # Write outputs (single CSV file each, for easy viewing/committing back)
    branch_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/branch_summary_csv"
    )
    flagged.select(
        "StudentID", "Name", "Branch", "Semester", "Attendance", "AvgMarks", "AtRisk"
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_dir}/full_report_csv"
    )

    elapsed = time.time() - t0

    summary = {
        "total_students": total_students,
        "total_at_risk": total_at_risk,
        "at_risk_percentage": round(100 * total_at_risk / total_students, 2),
        "runtime_seconds": round(elapsed, 2),
    }
    return summary


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # keep console output clean for the demo

    summary = run_pipeline(
        spark,
        students_path="data/students.csv",
        marks_path="data/marks.csv",
        output_dir="output",
    )

    print("\n=== Pipeline Run Summary ===")
    for k, v in summary.items():
        print(f"{k}: {v}")

    spark.stop()


if __name__ == "__main__":
    main()
