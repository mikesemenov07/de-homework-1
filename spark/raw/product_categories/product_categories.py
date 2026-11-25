import os

from pyspark.sql import SparkSession


CATEGORIES_PATH = "s3a://de-course/raw/product_categories"

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

query = """
    SELECT
        category_id,
        parent_category_id,
        category_name
    FROM public.product_categories
"""

categories = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load())

categories.write.format("parquet").mode("overwrite").save(CATEGORIES_PATH)

print(f"Сохранено: {CATEGORIES_PATH}")
spark.stop()