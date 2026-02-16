# create_test_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 1. Создаем пользователей
users = pd.DataFrame({
    'user_id': range(1, 101),
    'name': [f'User_{i}' for i in range(1, 101)],
    'created_at': [
        datetime(2025, np.random.randint(1, 13), np.random.randint(1, 28))
        for _ in range(100)
    ],
    'city': np.random.choice(['Moscow', 'SPB', 'Kazan', 'Novosibirsk'], 100)
})

# 2. Создаем магазины
stores = pd.DataFrame({
    'store_id': range(1, 21),
    'store_name': [f'Store_{i}' for i in range(1, 21)],
    'city': np.random.choice(['Moscow', 'SPB', 'Kazan', 'Novosibirsk'], 20)
})

# 3. Создаем заказы (1000 заказов)
orders = pd.DataFrame({
    'order_id': range(1, 1001),
    'user_id': np.random.randint(1, 101, 1000),
    'store_id': np.random.randint(1, 21, 1000),
    'amount': np.random.randint(100, 10000, 1000),
    'order_date': [
        datetime(2025, np.random.randint(1, 13), np.random.randint(1, 28))
        for _ in range(1000)
    ]
})

# Сохраняем как parquet
users.to_parquet('user.parquet')
stores.to_parquet('store.parquet')
orders.to_parquet('order.parquet')

print("Тестовые данные созданы!")