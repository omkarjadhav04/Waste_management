"""Spark (PySpark) engine: reads, cleans and analyses the waste data."""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NAME = "Apache Spark (PySpark)"
REQUIRED = ["date", "ward", "waste_type", "kg_collected", "recyclable_kg",
            "scheduled_pickup", "actual_pickup"]

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        _spark = (
            SparkSession.builder.appName("SmartWasteAnalytics")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.ansi.enabled", "false")  # bad values become NULL, not errors
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def load_and_clean(path):
    """Returns (cleaned_spark_df, stats_dict)."""
    spark = get_spark()
    raw = spark.read.option("header", True).csv(path)          # everything as string first
    raw = raw.toDF(*[c.strip().lower() for c in raw.columns])
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))

    rows_in = raw.count()
    df = raw.select(
        F.to_date(F.trim("date")).alias("date"),
        F.initcap(F.trim("ward")).alias("ward"),
        F.initcap(F.trim("waste_type")).alias("waste_type"),
        F.trim("kg_collected").cast("double").alias("kg_collected"),
        F.coalesce(F.trim("recyclable_kg").cast("double"), F.lit(0.0)).alias("recyclable_kg"),
        F.to_timestamp(F.trim("scheduled_pickup")).alias("scheduled_pickup"),
        F.to_timestamp(F.trim("actual_pickup")).alias("actual_pickup"),
    )
    df = df.filter(
        F.col("date").isNotNull()
        & F.col("ward").isNotNull() & (F.col("ward") != "")
        & F.col("waste_type").isNotNull() & (F.col("waste_type") != "")
        & F.col("kg_collected").isNotNull() & (F.col("kg_collected") > 0)
    )
    # recyclable can never be negative or more than what was collected
    df = df.withColumn(
        "recyclable_kg",
        F.greatest(F.lit(0.0), F.least(F.col("recyclable_kg"), F.col("kg_collected"))),
    )
    df = df.withColumn(
        "delay_minutes",
        (F.unix_timestamp("actual_pickup") - F.unix_timestamp("scheduled_pickup")) / 60.0,
    ).cache()
    rows_out = df.count()
    return df, {"rows_in": rows_in, "rows_out": rows_out, "rows_dropped": rows_in - rows_out}


def get_options(df):
    wards = sorted(r[0] for r in df.select("ward").distinct().collect())
    types = sorted(r[0] for r in df.select("waste_type").distinct().collect())
    bounds = df.agg(F.min("date"), F.max("date")).collect()[0]
    return wards, types, bounds[0], bounds[1]


def analyze(df, start, end, wards, types, threshold):
    f = df.filter(
        (F.col("date") >= F.lit(start)) & (F.col("date") <= F.lit(end))
        & F.col("ward").isin(wards) & F.col("waste_type").isin(types)
    ).withColumn(
        "delayed", F.when(F.col("delay_minutes") > threshold, 1).otherwise(0)
    )

    k = f.agg(
        F.sum("kg_collected").alias("total"),
        F.sum("recyclable_kg").alias("rec"),
        F.countDistinct("ward").alias("areas"),
        F.count("*").alias("pickups"),
        F.sum("delayed").alias("delayed"),
    ).collect()[0]
    total = float(k["total"] or 0)
    rec = float(k["rec"] or 0)
    pickups = int(k["pickups"] or 0)
    delayed = int(k["delayed"] or 0)
    kpi = {
        "total_kg": total,
        "recyclable_kg": rec,
        "recycling_pct": (rec / total * 100) if total else 0.0,
        "areas": int(k["areas"] or 0),
        "pickups": pickups,
        "delayed": delayed,
        "delay_rate": (delayed / pickups * 100) if pickups else 0.0,
    }

    by_ward = (f.groupBy("ward")
               .agg(F.sum("kg_collected").alias("total_kg"), F.sum("recyclable_kg").alias("recyclable_kg"))
               .withColumn("non_recyclable_kg", F.col("total_kg") - F.col("recyclable_kg"))
               .withColumn("recycling_pct", F.round(F.col("recyclable_kg") / F.col("total_kg") * 100, 1))
               .orderBy(F.desc("total_kg")).toPandas())
    by_type = (f.groupBy("waste_type")
               .agg(F.sum("kg_collected").alias("total_kg"), F.sum("recyclable_kg").alias("recyclable_kg"))
               .withColumn("recycling_pct", F.round(F.col("recyclable_kg") / F.col("total_kg") * 100, 1))
               .orderBy(F.desc("total_kg")).toPandas())
    by_date = (f.groupBy("date").agg(F.sum("kg_collected").alias("total_kg"))
               .orderBy("date").toPandas())
    ward_delay = (f.groupBy("ward")
                  .agg(F.count("*").alias("pickups"),
                       F.sum("delayed").alias("delayed_pickups"),
                       F.round(F.avg("delay_minutes"), 1).alias("avg_delay_min"))
                  .withColumn("delay_rate_pct", F.round(F.col("delayed_pickups") / F.col("pickups") * 100, 1))
                  .orderBy(F.desc("delayed_pickups")).toPandas())
    delays = (f.filter(F.col("delayed") == 1)
              .select("date", "ward", "waste_type", "scheduled_pickup", "actual_pickup", "delay_minutes")
              .orderBy(F.desc("delay_minutes")).limit(200).toPandas())
    rows = f.drop("delayed").orderBy("date", "ward").toPandas()
    return {"kpi": kpi, "by_ward": by_ward, "by_type": by_type, "by_date": by_date,
            "ward_delay": ward_delay, "delays": delays, "rows": rows}
