from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os
import glob


logger = logging.getLogger()
logger.setLevel('INFO')

DATA_DIR = '/opt/airflow/dags/input/'
PROCESSED_DIR = '/opt/airflow/dags/processed/'

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        files = glob.glob(self.filepath)
        if files:
            context['ti'].xcom_push(key='file_path', value=files[0])
            return True
        return False


def load_data_to_postgres(**kwargs):
    table_name = kwargs['table_name']
    ti = kwargs['ti']
    
    file_path = ti.xcom_pull(task_ids=f"wait_for_{table_name}", key='file_path')

    try:
        df = pd.read_csv(file_path)

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        engine = pg_hook.get_sqlalchemy_engine()

        df.to_sql(
            table_name,
            engine,
            schema='raw',
            if_exists='append',
            index=False
        )

        logger.info(f'Файл {file_path} загружен')

        processed_path = os.path.join(PROCESSED_DIR, os.path.basename(file_path))
        os.rename(file_path, processed_path)
        logger.info(f"Файл перемещён в {processed_path}")

    except:
        logger.error(f'Ошибка при загрузке {file_path}')
        raise


with DAG(
    'load_data_to_postgres',
    description = 'Вставка данных в постгрес из файлов',
    schedule_interval = '* * * * *',
    start_date=datetime(2025, 6, 26),
    catchup=False,
    max_active_runs=1
) as dag:
    
    wait_for_users = FileSensorWithXCom(
        task_id = 'wait_for_users',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}users_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_users_table = PostgresOperator(
        task_id = 'create_users_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.users (
                user_id TEXT,   
                name TEXT,      
                surname TEXT,   
                age INTEGER,       
                email TEXT,    
                phone TEXT ,     
                card_number TEXT
            );
        """
    )

    load_users_task = PythonOperator(
        task_id='load_users_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "users"},
        provide_context=True
    )

    wait_for_orders = FileSensorWithXCom(
        task_id = 'wait_for_orders',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}orders_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_orders_table = PostgresOperator(
        task_id = 'create_orders_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id INT,   
                product TEXT,      
                quantity INT,   
                price_per_unit DECIMAL(10, 2),       
                total_price DECIMAL(10, 2),    
                card_number TEXT,
                user_id TEXT
            );
        """
    )

    load_orders_task = PythonOperator(
        task_id='load_orders_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "orders"},
        provide_context=True
    )

    wait_for_deliveries = FileSensorWithXCom(
        task_id = 'wait_for_deliveries',
        fs_conn_id = 'fs_default',
        filepath = f'{DATA_DIR}deliveries_*.csv',
        poke_interval = 30,
        timeout = 30 * 5
    )

    create_deliveries_table = PostgresOperator(
        task_id = 'create_deliveries_table',
        postgres_conn_id = 'my_postgres_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS raw.deliveries (
                delivery_id TEXT,   
                order_id INT,      
                product TEXT,   
                company TEXT,       
                cost DECIMAL(10, 2),    
                courier_name TEXT,
                courier_phone TEXT,
                start_time DATE,
                end_time DATE,
                city TEXT,
                warehouse TEXT,
                address TEXT
            );
        """
    )

    load_deliveries_task = PythonOperator(
        task_id='load_deliveries_data',
        python_callable=load_data_to_postgres,
        op_kwargs={"table_name": "deliveries"},
        provide_context=True
    )


    wait_for_users >> create_users_table >> load_users_task >> wait_for_orders >> create_orders_table >> load_orders_task >> wait_for_deliveries >> create_deliveries_table >> load_deliveries_task