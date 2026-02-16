import findspark

findspark.init()


from pyspark.sql import SparkSession

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

spark = SparkSession.builder \
    .appName("Test Minio Connection") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Spark session created")

try:
    df = spark.read.parquet("s3a://data-lake/")
    print("✅ Connected to Minio successfully!")
except Exception as e:
    print(f"❌ Connection failed: {e}")

spark.stop()
