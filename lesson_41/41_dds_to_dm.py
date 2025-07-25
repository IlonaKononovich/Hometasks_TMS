from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from utils.product_validation import log_and_notify  # централизованное логирование и уведомления

# Уведомление о старте DAG-а
def notify_start():
    log_and_notify("DAG PRODUCT_dds_to_dm запущен.")

# Уведомление об успешном завершении DAG-а
def notify_success():
    log_and_notify("DAG PRODUCT_dds_to_dm завершён успешно.")

# Уведомление об ошибке с контекстом
def notify_failure(context):
    log_and_notify(f"Ошибка в PRODUCT_dds_to_dm: {context['exception']}", level='error')

# Функция расчёта метрик популярности товаров и загрузки в витрину данных
def calculate_and_load_popularity():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Получаем агрегированные данные из слоя dds
        query = """
            SELECT
                p.product_id,
                p.product_name,
                SUM(oi.quantity) AS total_quantity_sold,
                SUM(oi.quantity * oi.price) AS total_revenue,
                COUNT(DISTINCT oi.order_id) AS order_count,
                ROUND(AVG(oi.price), 2) AS avg_price
            FROM dds.order_items_product oi
            JOIN dds.products_product p ON oi.product_id = p.product_id
            GROUP BY p.product_id, p.product_name
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        # Вставляем или обновляем данные в витрине data_mart
        insert_sql = """
            INSERT INTO data_mart.dm_product_popularity (
                product_id,
                product_name,
                total_quantity_sold,
                total_revenue,
                order_count,
                avg_price
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                total_quantity_sold = EXCLUDED.total_quantity_sold,
                total_revenue = EXCLUDED.total_revenue,
                order_count = EXCLUDED.order_count,
                avg_price = EXCLUDED.avg_price
        """
        for row in rows:
            cursor.execute(insert_sql, row)

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

# Определение DAG с параметрами
with DAG(
    dag_id="PRODUCT_dds_to_dm",
    description="Построение витрины dm_product_popularity",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Уведомление о запуске DAG
    notify_start_task = PythonOperator(
        task_id='notify_start',
        python_callable=notify_start
    )

    # Группа задач по созданию таблицы и загрузке метрик в витрину
    with TaskGroup("dm_popularity_tasks", tooltip="Построение dm_product_popularity") as dm_group:

        # Создание схемы и таблицы для витрины, если не существуют
        create_dm_table = PostgresOperator(
            task_id="create_data_mart_product_popularity_table",
            postgres_conn_id="my_postgres_conn",
            sql="""
                CREATE SCHEMA IF NOT EXISTS data_mart;
                CREATE TABLE IF NOT EXISTS data_mart.dm_product_popularity (
                    product_id INTEGER PRIMARY KEY,
                    product_name TEXT,
                    total_quantity_sold INTEGER,
                    total_revenue NUMERIC(14, 2),
                    order_count INTEGER,
                    avg_price NUMERIC(10, 2)
                );
            """
        )

        # Загрузка рассчитанных метрик в таблицу витрины
        load_to_dm = PythonOperator(
            task_id="calculate_and_load_product_popularity",
            python_callable=calculate_and_load_popularity,
            on_failure_callback=notify_failure
        )

        create_dm_table >> load_to_dm

    # Уведомление об успешном завершении DAG
    notify_success_task = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success
    )

    # Последовательность выполнения задач
    notify_start_task >> dm_group >> notify_success_task
