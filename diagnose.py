import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# Настройки
MINIO_ENDPOINT = 'http://localhost:9000'
ACCESS_KEY = 'admin'
SECRET_KEY = 'password123'
BUCKET_NAME = 'data-lake'

# Создаем клиент
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

print("=" * 50)
print("ПРОВЕРКА ПОДКЛЮЧЕНИЯ К MINIO")
print("=" * 50)

# 1. Проверка списка bucket-ов
try:
    response = s3.list_buckets()
    print("\n✅ Подключение успешно!")
    print(f"Найдено bucket-ов: {len(response['Buckets'])}")
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']}")
except Exception as e:
    print(f"\n❌ Ошибка подключения: {e}")

# 2. Проверка конкретного bucket
try:
    s3.head_bucket(Bucket=BUCKET_NAME)
    print(f"\n✅ Bucket '{BUCKET_NAME}' существует и доступен")

    # 3. Список файлов в bucket
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' in response:
            print(f"\n📁 Файлы в bucket '{BUCKET_NAME}':")
            for obj in response['Contents']:
                print(f"  - {obj['Key']} ({obj['Size']} bytes)")
        else:
            print(f"\n📁 Bucket '{BUCKET_NAME}' пуст")
    except Exception as e:
        print(f"\n❌ Ошибка при получении списка файлов: {e}")

except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        print(f"\n❌ Bucket '{BUCKET_NAME}' не существует")
    elif error_code == '403':
        print(f"\n❌ Нет доступа к bucket '{BUCKET_NAME}'")
    else:
        print(f"\n❌ Ошибка: {e}")

# 4. Тестовая загрузка файла (если нужно)
print("\n" + "=" * 50)
answer = input("Хотите создать тестовый файл и загрузить его? (y/n): ")
if answer.lower() == 'y':
    # Создаем тестовый файл
    with open('test_file.txt', 'w') as f:
        f.write('Hello from boto3!')

    try:
        s3.upload_file(
            Filename='test_file.txt',
            Bucket=BUCKET_NAME,
            Key='test/test_file.txt'
        )
        print("✅ Тестовый файл загружен")

        # Проверяем что загрузилось
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix='test/')
        if 'Contents' in response:
            print("✅ Файл виден в bucket")

    except Exception as e:
        print(f"❌ Ошибка загрузки: {e}")