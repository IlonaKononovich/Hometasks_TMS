# Импорты необходимых модулей
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Работа с MinIO/S3
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Работа с PostgreSQL
from datetime import datetime
import pandas as pd
from utils.product_validation import log_and_notify  # Кастомная функция логирования и уведомлений

# Основная функция, которая извлекает новые файлы из Minio и загружает их в PostgreSQL
def get_and_load():
    log_and_notify("Началась загрузка файлов из Minio", "info")

    # Инициализация S3/MinIO хука и клиента
    s3_hook = S3Hook(aws_conn_id='minio_default')
    bucket_name = 'data-bucket'
    minio_client = s3_hook.get_conn()

    # Подключение к PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    engine = pg_hook.get_sqlalchemy_engine()
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Получаем список уже обработанных файлов из таблицы
    cursor.execute("SELECT file_name FROM raw.processed_minio_files")
    processed_files = {row[0] for row in cursor.fetchall()}

    new_files = 0  # Счётчик новых файлов
    errors = []    # Список ошибок

    # Получаем список файлов в бакете с префиксом "raw/"
    response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix='raw/')
    for obj in response.get('Contents', []):
        filekey = obj['Key']

        # Пропускаем, если файл уже был загружен ранее
        if filekey in processed_files:
            continue

        try:
            # Загружаем файл из Minio
            obj_data = minio_client.get_object(Bucket=bucket_name, Key=filekey)
            df = pd.read_csv(obj_data['Body'])  # Преобразуем в DataFrame

            if df.empty:
                continue  # Пропускаем пустые файлы

            # Загружаем данные в таблицу raw.minio_data
            df.to_sql('minio_data', engine, schema='raw', if_exists='append', index=False)

            # Добавляем файл в таблицу обработанных, чтобы не загружать повторно
            cursor.execute(
                "INSERT INTO raw.processed_minio_files (file_name) VALUES (%s) ON CONFLICT DO NOTHING",
                (filekey,)
            )
            new_files += 1

        except Exception as e:
            errors.append(f"{filekey}: {str(e)}")  # Логируем ошибку

    # Завершаем транзакцию и закрываем соединения
    conn.commit()
    cursor.close()
    conn.close()

    # Отправляем уведомление об успешной загрузке или её отсутствии
    if new_files > 0:
        log_and_notify(f"Обработка завершена: загружено {new_files} новых файлов", "info")
    else:
        log_and_notify("Новых файлов не обнаружено", "info")

    # Отправляем уведомления об ошибках, если они были
    if errors:
        for err in errors:
            log_and_notify(f"Ошибка при загрузке файла: {err}", "error")


# Описание DAG-а
with DAG(
    dag_id='MINIO_check_files_and_load_to_postgres',
    description='Проверка файлов и загрузка из Minio в Postgres',
    start_date=datetime(2025, 7, 16),  # Дата старта DAG-а
    schedule_interval=None,  # Запуск только по триггеру, вручную или из другого DAG-а
    catchup=False,  # Не выполнять пропущенные DAG-и
    tags=['minio']  # Теги для UI Airflow
) as dag:
    
    # Оператор, вызывающий основную функцию загрузки
    transfer_task = PythonOperator(
        task_id='transfer_task',
        python_callable=get_and_load
    )
