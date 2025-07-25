from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import random
import os
import pandas as pd

# Централизованное логирование и уведомления
from utils.product_validation import log_and_notify 

# Каталог для хранения сгенерированных CSV-файлов
DATA_DIR = '/opt/airflow/dags/PRODUCT_input/'
os.makedirs(DATA_DIR, exist_ok=True)

# Уведомление о старте DAG-а
def notify_start():
    log_and_notify("DAG PRODUCT_generate_data запущен.")

# Уведомление об успешном завершении DAG-а
def notify_success():
    log_and_notify("DAG PRODUCT_generate_data завершён успешно.")

# Генерация данных продуктов и сохранение в CSV
def generate_product_data():
    try:
        products = [{"product_id": i, "product_name": f"Product_{i}"} for i in range(1, 21)]
        df = pd.DataFrame(products)

        file_path = os.path.join(DATA_DIR, f'raw_products_{datetime.now():%Y%m%d%H%M%S}.csv')
        df.to_csv(file_path, index=False)

        log_and_notify(f"Сгенерированы данные продуктов: {len(df)} строк.")
    except Exception as e:
        log_and_notify(f"Ошибка при генерации продуктов: {e}", level='error')
        raise

# Генерация заказов и позиций заказов, сохранение в отдельные CSV
def generate_orders_data():
    try:
        orders, order_items = [], []
        products = list(range(1, 21))

        for order_id in range(1, 101):
            order_date = datetime(2025, 7, 1) + timedelta(days=random.randint(0, 14))
            orders.append({"order_id": order_id, "order_date": order_date.date()})

            for _ in range(random.randint(1, 5)):
                order_items.append({
                    "order_id": order_id,
                    "product_id": random.choice(products),
                    "quantity": random.randint(1, 5),
                    "price": round(random.uniform(100, 1000), 2)
                })

        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        pd.DataFrame(orders).to_csv(os.path.join(DATA_DIR, f'raw_orders_{timestamp}.csv'), index=False)
        pd.DataFrame(order_items).to_csv(os.path.join(DATA_DIR, f'raw_order_items_{timestamp}.csv'), index=False)

        log_and_notify(f"Сгенерированы данные заказов: {len(orders)} строк.")
        log_and_notify(f"Сгенерированы данные позиций: {len(order_items)} строк.")
    except Exception as e:
        log_and_notify(f"Ошибка при генерации заказов: {e}", level='error')
        raise


# Определение DAG-а генерации данных
with DAG(
    dag_id="PRODUCT_generate_data",
    description="Генерация сырых CSV-файлов для витрины популярности товаров",
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False
) as dag:

    notify_start = PythonOperator(
        task_id='notify_start',
        python_callable=notify_start
    )

    # Группа задач генерации CSV-файлов
    with TaskGroup("generate_data_group", tooltip="Генерация CSV-файлов") as generate_data_group:
        generate_products = PythonOperator(
            task_id='generate_products',
            python_callable=generate_product_data
        )

        generate_orders = PythonOperator(
            task_id='generate_orders',
            python_callable=generate_orders_data
        )

        generate_products >> generate_orders

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success
    )

    notify_start >> generate_data_group >> notify_success
