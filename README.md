# ETL: Top-3 магазинов по сумме заказов (Spark + MinIO)

Прототип решает задачу из ТЗ: **для каждого города (`store.city`) найти топ‑3 магазинов по сумме заказов (`order.amount`)**, сделанных **пользователями, зарегистрированными в 2025 году** (`user.created_at`).

Вход: 3 Parquet-файла (`user` / `store` / `order`) в S3‑совместимом хранилище (MinIO).  
Выход: Parquet c результатом в MinIO.

## Структура проекта

- `docker-compose.yaml` — поднимает MinIO и контейнер со Spark
- `app/spark_job.py` — Spark ETL (читает из MinIO, считает топ‑3, пишет обратно в MinIO)
- `upload_to_minio.py` — генерирует тестовые данные и загружает их в MinIO
- `check_result.py` — скачивает результат из MinIO и печатает его
- `create_data.py` — локальная генерация parquet (без загрузки в MinIO)
- `diagnose.py` / `test-conn.py` — вспомогательные диагностические скрипты

## Требования

- Docker Desktop (Linux containers)
- Python + venv (для `upload_to_minio.py` и `check_result.py`)

## Быстрый старт (Windows / PowerShell)

Поднять инфраструктуру:

```powershell
cd .   # корень репозитория
docker compose up -d
```

MinIO:
- **S3 API**: `http://localhost:9000`
- **Console**: `http://localhost:9001`
- **логин/пароль**: `admin` / `password123` (см. `docker-compose.yaml`)

Сгенерировать и загрузить входные данные в MinIO (bucket `data-lake`, префикс `input/`):

```powershell
.\.venv\Scripts\python.exe upload_to_minio.py
```

Запустить ETL в контейнере Spark (важно: добавляем S3A зависимости через `--packages`):

```powershell
docker exec spark-app /opt/spark/bin/spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
  /opt/spark/app/spark_job.py
```

Проверить результат (скачает part‑файлы из `output/top_stores.parquet/` и выведет таблицу):

```powershell
.\.venv\Scripts\python.exe check_result.py
```

## Python-зависимости (для `upload_to_minio.py` / `check_result.py`)

Если у вас ещё нет окружения:

```powershell
python -m venv .venv
.\.venv\Scripts\pip install boto3 pandas numpy pyarrow
```

## Формат данных

### Вход (MinIO)

Bucket: `data-lake`

- `input/user.parquet`
- `input/store.parquet`
- `input/order.parquet`

Ожидаемая схема по ТЗ:
- **user**: `id`, `name`, `phone`, `created_at`
- **store**: `id`, `name`, `city`
- **order**: `id`, `amount`, `user_id`, `store_id`, `status`, `created_at`

Примечание: `app/spark_job.py` **нормализует имена колонок**, поэтому поддерживает варианты:
- `user.id` → `user_id`
- `store.id` → `store_id`
- `store.name` → `store_name`

### Выход (MinIO)

- `output/top_stores.parquet/` — директория parquet (Spark пишет в виде набора `part-*.parquet`)

Схема результата:
- `city` (varchar)
- `store_name` (varchar)
- `target_amount` (decimal/number) — сумма `order.amount`

## Как загрузить свои данные

Положите ваши Parquet в MinIO в bucket `data-lake` под ключи:
- `input/user.parquet`
- `input/store.parquet`
- `input/order.parquet`

Дальше просто запускайте `spark_job.py` как в “Быстрый старт”.

## Остановка

```powershell
docker compose down
```

