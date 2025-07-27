# Импорт необходимых модулей
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Хук для работы с MinIO
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Оператор для запуска другого DAG-а
from datetime import datetime
import pandas as pd
import random
import os

# Функция для генерации CSV-файла и загрузки его в MinIO
def generate_data(**kwargs):
    # Создаём DataFrame с 10 строками случайных значений
    df = pd.DataFrame({'id': range(1, 11), 'value': [random.randint(1, 100) for _ in range(10)]})

    # Сохраняем временный файл на диск
    temp_file_path = '/tmp/data.csv' 
    df.to_csv(temp_file_path, index=False)

    # Генерируем уникальное имя файла с меткой времени
    curr_time = kwargs['execution_date'].strftime("%Y%m%d_%H%M%S")
    filename = f'data_{curr_time}.csv'

    # Получаем хук и подключаемся к MinIO (через AWS API)
    hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'data-bucket'

    # Создаём бакет, если его ещё не существует
    if not hook.check_for_bucket(bucket_name):
        hook.create_bucket(bucket_name)

    # Загружаем CSV-файл в MinIO в папку /raw/
    hook.load_file(
        filename=temp_file_path,
        bucket_name=bucket_name,
        key='/raw/' + filename,
        replace=False  # Не перезаписываем файл, если он уже есть
    )

    # Удаляем временный файл
    os.remove(temp_file_path)

# Определение DAG-а
with DAG(
    '43_Upload_file_csv_to_minio',  # Уникальный ID DAG-а
    start_date=datetime(2025, 7, 16),  # Дата старта
    schedule_interval=None,  # DAG запускается только вручную
    catchup=False,  # Не выполнять DAG задним числом
    tags=['minio']  # Метка для фильтрации в интерфейсе Airflow
) as dag:
    
    # Задача генерации и загрузки CSV-файла в MinIO
    generate_data_task = PythonOperator(
        task_id='generate_load_csv_file_to_minio',
        python_callable=generate_data
    )

    # Оператор запуска следующего DAG-а после генерации файла
    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_load_to_postgres",  # Название задачи
        trigger_dag_id="MINIO_check_files_and_load_to_postgres",  # ID DAG-а, который нужно запустить
        wait_for_completion=False,  # Не ждать завершения второго DAG-а
        reset_dag_run=True  # Если DAG уже запускался — перезапустить заново
    )

    # Устанавливаем последовательность задач: сначала генерация, затем триггер
    generate_data_task >> trigger_next_dag
