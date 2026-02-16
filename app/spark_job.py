from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, rank, year
from pyspark.sql.window import Window

print("🚀 Запуск Spark приложения...")

# ... (начальная часть с созданием SparkSession остается без изменений) ...
spark = (
    SparkSession.builder
    .appName("Top 3 Stores by City") \
    # В официальном образе Spark нет S3A коннектора по умолчанию — подтягиваем зависимости
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    ) \
    .getOrCreate()
)

print("✅ SparkSession создана")

try:
    # 1. Читаем данные из Minio (без изменений)
    print("\n📖 Читаем данные из Minio...")
    users = spark.read.parquet("s3a://data-lake/input/user.parquet")
    stores = spark.read.parquet("s3a://data-lake/input/store.parquet")
    orders = spark.read.parquet("s3a://data-lake/input/order.parquet")

    # Нормализуем схему под ТЗ:
    # user: id,name,phone,created_at
    # store: id,name,city
    # order: id,amount,user_id,store_id,status,created_at
    if "id" in users.columns and "user_id" not in users.columns:
        users = users.withColumnRenamed("id", "user_id")
    if "id" in stores.columns and "store_id" not in stores.columns:
        stores = stores.withColumnRenamed("id", "store_id")
    if "name" in stores.columns and "store_name" not in stores.columns:
        stores = stores.withColumnRenamed("name", "store_name")
    if "id" in orders.columns and "order_id" not in orders.columns:
        orders = orders.withColumnRenamed("id", "order_id")

    print(f"   Пользователей: {users.count()}")
    print(f"   Магазинов: {stores.count()}")
    print(f"   Заказов: {orders.count()}")

    # 2. Фильтруем пользователей 2025 года (без изменений)
    print("\n🔍 Фильтруем пользователей 2025 года...")
    users_2025 = users.filter(year("created_at") == 2025)
    print(f"   Пользователей 2025 года: {users_2025.count()}")

    # 3. Джойним таблицы
    print("\n🔗 Соединяем таблицы...")
    # Сначала соединяем заказы с отфильтрованными пользователями
    orders_with_users = orders.join(users_2025, "user_id", "inner")

    # Затем соединяем с магазинами. Явно указываем, какие столбцы брать после JOIN,
    # чтобы избежать конфликта имен 'city'.
    # Мы будем использовать город из таблицы магазинов (stores.city) как основной.
    joined_data = orders_with_users.join(stores, "store_id", "inner") \
        .select(
        orders_with_users["*"],  # все поля из orders_with_users
        stores["store_name"],  # название магазина
        stores["city"].alias("store_city")  # город магазина с понятным алиасом
    )

    # 4. Агрегируем: группируем по городу магазина (store_city) и магазину
    print("\n📊 Агрегируем данные...")
    result = joined_data.groupBy("store_id", "store_name", "store_city") \
        .agg(sum("amount").alias("target_amount"))

    # 5. Добавляем ранг и берем топ-3 по каждому городу
    print("\n🏆 Вычисляем топ-3 магазина по городам...")
    window_spec = Window.partitionBy("store_city").orderBy(col("target_amount").desc())
    top_stores = result \
        .withColumn("rank", rank().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .drop("rank") \
        .withColumnRenamed("store_city", "city")  # Переименовываем обратно для красоты

    # 6. Сохраняем результат (без изменений)
    print("\n💾 Сохраняем результат в Minio...")
    top_stores.write \
        .mode("overwrite") \
        .parquet("s3a://data-lake/output/top_stores.parquet")
    print("✅ Результат сохранен в s3a://data-lake/output/top_stores.parquet")

    # 7. Показываем результат (без изменений)
    print("\n📊 Топ-3 магазина по городам:")
    print("=" * 60)
    top_stores.show(20, truncate=False)
    print("=" * 60)

except Exception as e:
    print(f"❌ Ошибка: {e}")
    import traceback

    traceback.print_exc()

finally:
    spark.stop()
    print("\n🏁 Spark остановлен")