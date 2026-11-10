from io import StringIO
from pathlib import Path
import psycopg2


admin = psycopg2.connect(host="greenplum", user="gpadmin", dbname="postgres")
admin.autocommit = True

with admin.cursor() as cursor:
    cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'dwh'")
    if cursor.fetchone() is None:
        cursor.execute("CREATE DATABASE dwh")
admin.close()

source = psycopg2.connect(host="postgres", dbname="shop", user="student", password="studentpass")
source.set_session(isolation_level="REPEATABLE READ", readonly=True)
target = psycopg2.connect(host="greenplum", dbname="dwh", user="gpadmin")
tables = ["customers", "products", "product_categories", "orders", "order_items", "payments"]
ddl = Path(__file__).resolve().parents[1] / "init/greenplum/load_raw.sql"
counts = {}

with source, target:
    with source.cursor() as src, target.cursor() as dst:
        dst.execute(ddl.read_text(encoding="utf-8"))
        for table in tables:
            dst.execute(f"TRUNCATE TABLE raw.{table}")
            with StringIO() as data:
                src.copy_expert(f"COPY public.{table} TO STDOUT WITH CSV", data)
                data.seek(0)
                dst.copy_expert(f"COPY raw.{table} FROM STDIN WITH CSV", data)
            dst.execute(f"ANALYZE raw.{table}")
            dst.execute(f"SELECT count(*) FROM raw.{table}")
            counts[table] = dst.fetchone()[0]

source.close()
target.close()




for table, count in counts.items():
    print(f"{table}: {count}")