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

RESULT_PATH = "s3a://de-course/intermediate/int_orders_with_payments"

spark = SparkSession.builder.config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"]).config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"]).config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"]).config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet("s3a://de-course/staging/stg_orders").filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date))
payments = spark.read.parquet("s3a://de-course/staging/stg_payments").filter(((F.col("paid_dt") >= start_date) & (F.col("paid_dt") < load_date)) | F.col("paid_dt").isNull())

is_payment = (F.col("payment_status") == "success") & ~F.col("is_refund")

order_payments = payments.groupBy("order_id").agg(
    F.count(F.when(is_payment, 1)).alias("successful_payments_count"),
    F.coalesce(F.sum(F.when(is_payment, F.col("amount"))), F.lit(0)).alias("paid_amount"),
    F.coalesce(-F.sum(F.when(F.col("is_refund"), F.col("amount"))), F.lit(0)).alias("refunded_amount"),
    F.min(F.when(is_payment, F.col("paid_at"))).alias("first_paid_at"),
    F.count(F.when(F.col("payment_status") == "success", 1)).alias("success_operations_count"),
    F.count(F.when(F.col("payment_status") == "failed", 1)).alias("failed_operations_count"),
    F.count(F.when(F.col("payment_status") == "pending", 1)).alias("pending_operations_count"))

result = orders.join(order_payments, on="order_id", how="left")

zero_columns = ["successful_payments_count", "paid_amount", "refunded_amount", "success_operations_count", "failed_operations_count", "pending_operations_count"]

for column in zero_columns:
    result = result.withColumn(column, F.coalesce(F.col(column), F.lit(0)))

result = result.withColumn("net_paid_amount", F.col("paid_amount") - F.col("refunded_amount")).withColumn("is_confirmed", (F.col("order_status") == "paid") & (F.col("successful_payments_count") > 0))

result = result.select("order_id", "customer_id", "ordered_at", "order_date", "order_status", "order_amount", "is_test_order", "successful_payments_count", "paid_amount", "refunded_amount", "net_paid_amount", "first_paid_at", "is_confirmed", "success_operations_count", "failed_operations_count", "pending_operations_count")

result.write.format("parquet").mode("overwrite").partitionBy("order_date").save(RESULT_PATH)

print(f"Сохранено: {RESULT_PATH}")
spark.stop()