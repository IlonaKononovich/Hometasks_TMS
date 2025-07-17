from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.product_validation import log_and_notify  # Telegram-логирование

# Уведомление о старте DAG-а
def notify_start():
    log_and_notify("DAG PRODUCT_raw_to_dds запущен.")

# Уведомление об успешном завершении DAG-а
def notify_success():
    log_and_notify("DAG PRODUCT_raw_to_dds завершён успешно.")

# Уведомление об ошибке с контекстом
def notify_failure(context):
    log_and_notify(f"Ошибка в PRODUCT_raw_to_dds: {context['exception']}", level='error')

# Перенос данных из raw в dds с upsert'ом
def raw_to_dds():
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        # Загрузка продуктов
        cursor.execute("SELECT product_id, product_name FROM raw.products_product")
        for product_id, product_name in cursor.fetchall():
            cursor.execute("""
                INSERT INTO dds.products_product (product_id, product_name)
                VALUES (%s, %s)
                ON CONFLICT (product_id) DO UPDATE 
                    SET product_name = EXCLUDED.product_name
            """, (product_id, product_name))

        # Загрузка заказов
        cursor.execute("SELECT order_id, order_date FROM raw.orders_product")
        for order_id, order_date in cursor.fetchall():
            cursor.execute("""
                INSERT INTO dds.orders_product (order_id, order_date)
                VALUES (%s, %s)
                ON CONFLICT (order_id) DO UPDATE 
                    SET order_date = EXCLUDED.order_date
            """, (order_id, order_date))

        # Загрузка позиций заказов
        cursor.execute("SELECT order_id, product_id, quantity, price FROM raw.order_items_product")
        for order_id, product_id, quantity, price in cursor.fetchall():
            cursor.execute("""
                INSERT INTO dds.order_items_product (order_id, product_id, quantity, price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (order_id, product_id) DO UPDATE 
                    SET quantity = EXCLUDED.quantity, price = EXCLUDED.price
            """, (order_id, product_id, quantity, price))

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

# Описание DAG-а и последовательность задач
with DAG(
    dag_id="PRODUCT_raw_to_dds",
    description="Загрузка данных из raw в dds для витрины popular products",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    notify_start = PythonOperator(
        task_id='notify_start',
        python_callable=notify_start
    )

    load_raw_to_dds = PythonOperator(
        task_id='load_raw_to_dds',
        python_callable=raw_to_dds,
        on_failure_callback=notify_failure
    )

    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success
    )

    notify_start >> load_raw_to_dds >> notify_success
