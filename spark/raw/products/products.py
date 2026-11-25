import os
from pyspark.sql import SparkSession


PRODUCTS_PATH = "s3a://de-course/raw/products"

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
    driver="org.postgresql.Driver",
)

query = """
    SELECT
        product_id,
        category,
        brand,
        product_name
    FROM public.products
"""

products = (spark.read.format("jdbc").option("query", query).options(**jdbc).load())

products.write.format("parquet").mode("overwrite").save(PRODUCTS_PATH)

print(f"Сохранено: {PRODUCTS_PATH}")
spark.stop()