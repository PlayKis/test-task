# check_result.py
# Скачивает результат из MinIO и печатает его как таблицу.
import os

import boto3
from botocore.client import Config
import pandas as pd

ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "password123"
BUCKET = "data-lake"
PREFIX = "output/top_stores.parquet/"

OUT_DIR = os.path.join(os.getcwd(), "result", "top_stores")
os.makedirs(OUT_DIR, exist_ok=True)

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]

if not keys:
    raise SystemExit(
        f"Не найдено parquet-частей по префиксу s3://{BUCKET}/{PREFIX}. "
        "Сначала запустите ETL (spark_job.py)."
    )

for key in keys:
    filename = os.path.basename(key)
    local_path = os.path.join(OUT_DIR, filename)
    s3.download_file(BUCKET, key, local_path)

# Spark пишет parquet как директорию с part-файлами — pandas/pyarrow умеет читать директорию
df = pd.read_parquet(OUT_DIR)
print(df)