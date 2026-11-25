import os
from pyspark.sql import SparkSession


ITEMS_PATH = "s3a://de-course/raw/order_items"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

jdbc = dict(
    url=f"jdbc:postgresql://postgres:5432/{os.environ['POSTGRES_DB']}",
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    driver="org.postgresql.Driver")

# В таблице нет даты изменения, поэтому выбрать окно в три дня нельзя.
# Каждый раз читаем и заменяем всю таблицу.
query = """
    SELECT
        order_id,
        product_id,
        quantity,
        unit_price,
        discount_amount
    FROM public.order_items
"""

items = (spark.read.format("jdbc").option("query", query).options(**jdbc).load())

items.write.format("parquet").mode("overwrite").save(ITEMS_PATH)

print(f"Сохранено: {ITEMS_PATH}")
spark.stop()