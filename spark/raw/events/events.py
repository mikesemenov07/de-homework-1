import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True)
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

EVENTS_PATH = "s3a://de-course/raw/events"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def sink_has_data(path):
    try:
        return spark.read.parquet(path).limit(1).count() > 0
    except Exception:
        return False


if sink_has_data(EVENTS_PATH):
    print(f"Инкремент: [{start_date}; {load_date})")
    where = f"""
        WHERE event_time >= '{start_date}'::timestamp
          AND event_time < '{load_date}'::timestamp
    """
else:
    print("Приёмник пуст — полная загрузка")
    where = ""

query = f"""
    SELECT
        event_id,
        customer_id,
        event_time,
        event_type,
        product_id,
        session_id
    FROM public.events
    {where}
"""

jdbc = dict(
    url=f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}",
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    driver="org.postgresql.Driver",
)

events = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

# event_time в источнике NOT NULL, поэтому пустых дат не будет.
events = events.withColumn("event_dt", F.to_date("event_time"))

(events.write.format("parquet").mode("overwrite").partitionBy("event_dt").save(EVENTS_PATH))

print(f"Сохранено: {EVENTS_PATH}")
spark.stop()