# Импортируем необходимые классы и функции из Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from home_task_34.utils import time_of_generation, generate_users_file, generate_sales_txt, count_rows


default_args = dict(
    retries=3,                                 # Повторять при ошибках до 3 раз
    retry_delay=timedelta(minutes=60),         # Интервал между попытками
    execution_timeout=timedelta(hours=1)       # Максимальное время выполнения таска
)

# Определяем DAG
with DAG(
    dag_id='generate_two_files',                      # Уникальный id дага
    description='Даг, который генерирует два файла',  # Описание задачи
    schedule_interval='*/2 * * * *',                  # Запускать каждые 2 минуты
    start_date=datetime(2025, 6, 1),                  # Дата начала работы дага
    catchup=False,                                    # Не запускать задачи за прошлое
    default_args=default_args
) as dag:
    # Оператор запускает функцию time_of_generation
    task1 = PythonOperator(
        task_id='start_generation_time',          
        python_callable=time_of_generation,       
    )
    # Оператор для создания Excel файла пользователей
    task2 = PythonOperator(
        task_id='generate_users_file',
        python_callable=generate_users_file,
    )
    # Оператор для создания текстового файла продаж
    task3 = PythonOperator(
        task_id='generate_sales_file',
        python_callable=generate_sales_txt,
    )
    # Оператор для подсчёта и вывода количества строк в файлах
    task4 = PythonOperator(
        task_id='read_files_and_print_amount_rows',
        python_callable=count_rows,
    )

    # Определяем порядок выполнения тасков:
    # сначала task1,
    # затем параллельно task2 и task3,
    # затем task4 после завершения обоих
    task1 >> [task2, task3] >> task4
