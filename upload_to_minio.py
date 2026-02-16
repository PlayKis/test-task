# upload_to_minio.py
import sys
import boto3
from botocore.client import Config
import pandas as pd
import numpy as np
from datetime import datetime

# Чтобы скрипт не падал в Windows-консоли из-за Unicode/эмодзи
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# Проверка наличия библиотеки для parquet
try:
    import pyarrow

    print("pyarrow установлен")
except ImportError:
    try:
        import fastparquet

        print("fastparquet установлен")
    except ImportError:
        print("Не найдена библиотека для Parquet. Установите pyarrow:")
        print("pip install pyarrow")
        exit(1)

print("Создаем тестовые данные...")

# Пользователи
users = pd.DataFrame({
    'user_id': range(1, 101),
    'name': [f'User_{i}' for i in range(1, 101)],
    'created_at': [datetime(2025, np.random.randint(1, 13), np.random.randint(1, 28))
                   for _ in range(100)],
    'city': np.random.choice(['Moscow', 'SPB', 'Kazan', 'Novosibirsk'], 100)
})

# Магазины
stores = pd.DataFrame({
    'store_id': range(1, 21),
    'store_name': [f'Store_{i}' for i in range(1, 21)],
    'city': np.random.choice(['Moscow', 'SPB', 'Kazan', 'Novosibirsk'], 20)
})

# Заказы
orders = pd.DataFrame({
    'order_id': range(1, 1001),
    'user_id': np.random.randint(1, 101, 1000),
    'store_id': np.random.randint(1, 21, 1000),
    'amount': np.random.randint(100, 10000, 1000),
    'order_date': [datetime(2025, np.random.randint(1, 13), np.random.randint(1, 28))
                   for _ in range(1000)]
})

# Сохраняем как parquet
print("Сохраняем в Parquet...")
users.to_parquet('user.parquet', index=False)
stores.to_parquet('store.parquet', index=False)
orders.to_parquet('order.parquet', index=False)
print("Данные сохранены как parquet")

# Загружаем в Minio
print("Загружаем в Minio...")
try:
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='password123',
        config=Config(signature_version='s3v4')
    )

    # Создаем bucket если нет
    try:
        s3.create_bucket(Bucket='data-lake')
        print("Bucket создан")
    except Exception as e:
        if 'BucketAlreadyOwnedByYou' in str(e) or 'BucketAlreadyExists' in str(e):
            print("Bucket уже существует")
        else:
            print(f"Ошибка при создании bucket: {e}")

    # Загружаем файлы
    for file in ['user.parquet', 'store.parquet', 'order.parquet']:
        s3.upload_file(file, 'data-lake', f'input/{file}')
        print(f"{file} загружен в data-lake/input/{file}")

    # Проверяем загруженные файлы
    response = s3.list_objects_v2(Bucket='data-lake', Prefix='input/')
    print("\nФайлы в bucket:")
    for obj in response.get('Contents', []):
        print(f"  - {obj['Key']} ({obj['Size']} bytes)")

except Exception as e:
    print(f"Ошибка при загрузке в Minio: {e}")
    print("\nПроверьте:")
    print("1. Запущен ли Minio: docker ps")
    print("2. Доступен ли Minio по адресу localhost:9000")
    print("3. Правильные ли логин/пароль (admin/password123)")

print("Скрипт завершен!")