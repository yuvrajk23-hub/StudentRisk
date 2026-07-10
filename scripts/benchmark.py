"""
benchmark.py
-------------
Compares plain Pandas (single-threaded) vs PySpark (parallel across cores)
on the SAME aggregation task, at increasing data sizes. This is the
"why does distributed computing matter" proof for the viva -- at small
sizes Pandas wins (Spark has startup/scheduling overhead), but as data
grows, Spark's parallel execution starts to pay off.

Run:  python scripts/benchmark.py
"""

import time
import random
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

random.seed(1)


def make_pandas_df(n):
    branches = ["CSE", "ECE", "AI", "Cloud", "Mechanical"]
    return pd.DataFrame(
        {
            "StudentID": [f"S{i:07d}" for i in range(n)],
            "Branch": [branches[i % 5] for i in range(n)],
            "Attendance": [round(random.triangular(35, 99, 82), 1) for _ in range(n)],
            "Marks": [round(random.triangular(2, 100, 60), 1) for _ in range(n)],
        }
    )


def run_pandas(df):
    t0 = time.time()
    result = (
        df[df["Attendance"] >= 40]
        .groupby("Branch")
        .agg(AvgMarks=("Marks", "mean"), Count=("StudentID", "count"))
        .sort_values("AvgMarks")
    )
    elapsed = time.time() - t0
    return elapsed, result


def run_pyspark(spark, pandas_df):
    t0 = time.time()
    sdf = spark.createDataFrame(pandas_df)  # Arrow-accelerated conversion
    result = (
        sdf.filter(sdf.Attendance >= 40)
        .groupBy("Branch")
        .agg(avg("Marks").alias("AvgMarks"), count("StudentID").alias("Count"))
        .orderBy("AvgMarks")
    )
    result.collect()  # action -> triggers execution, comparable to pandas above
    elapsed = time.time() - t0
    return elapsed


def main():
    spark = (
        SparkSession.builder.appName("Benchmark")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    sizes = [1_000, 20_000, 100_000, 500_000]
    print(f"{'Rows':>10} | {'Pandas (s)':>12} | {'PySpark (s)':>12} | Faster")
    print("-" * 55)

    results = []
    for n in sizes:
        pdf = make_pandas_df(n)
        pandas_time, _ = run_pandas(pdf)
        spark_time = run_pyspark(spark, pdf)
        faster = "PySpark" if spark_time < pandas_time else "Pandas"
        results.append((n, pandas_time, spark_time, faster))
        print(f"{n:>10} | {pandas_time:>12.3f} | {spark_time:>12.3f} | {faster}")

    spark.stop()

    with open("output/benchmark_results.csv", "w") as f:
        f.write("Rows,PandasSeconds,PySparkSeconds,Faster\n")
        for n, p, s, w in results:
            f.write(f"{n},{p:.3f},{s:.3f},{w}\n")


if __name__ == "__main__":
    main()
