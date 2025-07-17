from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
import os
import glob

 # Централизованное логирование и уведомления
from utils.product_validation import validate_csv_file, log_and_notify  

# Константы путей 
DATA_DIR = '/opt/airflow/dags/PRODUCT_input/'
PROCESSED_DIR = '/opt/airflow/dags/PRODUCT_processed/'

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Уведомления
def notify_start():
    log_and_notify("DAG PRODUCT_LoadToRaw запущен.")

def notify_success():
    log_and_notify("DAG PRODUCT_LoadToRaw завершён успешно.")

def notify_failure(context):
    log_and_notify(f"DAG PRODUCT_LoadToRaw завершился с ошибкой: {context['exception']}", level="error")

# Кастомный сенсор с передачей пути файла через XCom 
class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context['ti'].xcom_push(key='file_path', value=files[0])
            return True
        return False

# Загрузка CSV-файла в таблицу PostgreSQL
def load_csv_to_postgres(table_name, **kwargs):
    try:
        ti = kwargs['ti']
        file_path = ti.xcom_pull(key='file_path', task_ids=f'wait_for_{table_name}')
        df = pd.read_csv(file_path)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(table_name, engine, schema='raw', if_exists='append', index=False)

        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        os.rename(file_path, processed_path)

        log_and_notify(f"{table_name}: файл загружен и перемещён.")
    except Exception as e:
        log_and_notify(f"{table_name}: ошибка загрузки файла: {e}", level="error")
        raise

# Определение DAG-а
with DAG(
    dag_id="PRODUCT_LoadToRaw",
    description="Загрузка файлов в raw с валидацией",
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False
) as dag:

    # Уведомление о запуске
    notify_start = PythonOperator(task_id="notify_start", python_callable=notify_start)

    # Сенсор и создание таблицы для products
    wait_products = FileSensorWithXCom(
        task_id='wait_for_products_product',
        fs_conn_id='fs_default',
        filepath=f'{DATA_DIR}raw_products_*.csv',
        poke_interval=10,
        timeout=60,
        on_failure_callback=notify_failure
    )

    create_products = PostgresOperator(
        task_id='create_products_product_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.products_product (
                product_id INTEGER,
                product_name TEXT
            );
        """,
        on_failure_callback=notify_failure
    )

    # Сенсор и создание таблицы для orders
    wait_orders = FileSensorWithXCom(
        task_id='wait_for_orders_product',
        fs_conn_id='fs_default',
        filepath=f'{DATA_DIR}raw_orders_*.csv',
        poke_interval=10,
        timeout=60,
        on_failure_callback=notify_failure
    )

    create_orders = PostgresOperator(
        task_id='create_orders_product_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.orders_product (
                order_id INTEGER,
                order_date DATE
            );
        """,
        on_failure_callback=notify_failure
    )

    # Сенсор и создание таблицы для order_items
    wait_items = FileSensorWithXCom(
        task_id='wait_for_order_items_product',
        fs_conn_id='fs_default',
        filepath=f'{DATA_DIR}raw_order_items_*.csv',
        poke_interval=10,
        timeout=60,
        on_failure_callback=notify_failure
    )

    create_items = PostgresOperator(
        task_id='create_order_items_product_table',
        postgres_conn_id='my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.order_items_product (
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                price NUMERIC(10, 2)
            );
        """,
        on_failure_callback=notify_failure
    )

    # Группа валидации CSV-файлов
    with TaskGroup("validate_files", tooltip="Валидация CSV") as validate_group:
        validate_products = PythonOperator(
            task_id='validate_products_product',
            python_callable=validate_csv_file,
            op_kwargs={'table_name': 'products_product'},
            on_failure_callback=notify_failure
        )
        validate_orders = PythonOperator(
            task_id='validate_orders_product',
            python_callable=validate_csv_file,
            op_kwargs={'table_name': 'orders_product'},
            on_failure_callback=notify_failure
        )
        validate_items = PythonOperator(
            task_id='validate_order_items_product',
            python_callable=validate_csv_file,
            op_kwargs={'table_name': 'order_items_product'},
            on_failure_callback=notify_failure
        )

    # Загрузка CSV-файлов в PostgreSQL
    load_products = PythonOperator(
        task_id='load_products_product',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'products_product'},
        on_failure_callback=notify_failure
    )

    load_orders = PythonOperator(
        task_id='load_orders_product',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'orders_product'},
        on_failure_callback=notify_failure
    )

    load_items = PythonOperator(
        task_id='load_order_items_product',
        python_callable=load_csv_to_postgres,
        op_kwargs={'table_name': 'order_items_product'},
        on_failure_callback=notify_failure
    )

    # Уведомление об успешном завершении
    notify_success = PythonOperator(task_id="notify_success", python_callable=notify_success)

    # Последовательность выполнения задач
    notify_start >> wait_products >> create_products >> validate_products >> load_products >> notify_success
    notify_start >> wait_orders >> create_orders >> validate_orders >> load_orders >> notify_success
    notify_start >> wait_items >> create_items >> validate_items >> load_items >> notify_success
