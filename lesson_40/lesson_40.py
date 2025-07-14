# Импорт необходимых модулей и операторов Airflow
from airflow import DAG  # Основной класс для создания DAG
from airflow.operators.python import PythonOperator  # Оператор для запуска Python-функций
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Оператор для выполнения SQL-запросов
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Хук для подключения к PostgreSQL
from airflow.utils.task_group import TaskGroup  # Для группировки задач
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Оператор для запуска другого DAG-а
from datetime import datetime  # Модуль для работы с датой и временем
import pandas as pd  # Библиотека для работы с таблицами (DataFrame)

# Функция для извлечения данных сессий из таблицы raw.sessions
def extract_session_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Подключение к PostgreSQL через Airflow Hook
    engine = pg_hook.get_sqlalchemy_engine()  # Получение SQLAlchemy engine для Pandas

    data = pd.read_sql('select * from raw.sessions', engine)  # Чтение таблицы sessions в DataFrame
    kwargs['ti'].xcom_push(key='sessions_data', value=data)  # Сохранение результата в XCom

# Функция для извлечения данных событий из таблицы raw.events
def extract_events_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Подключение к PostgreSQL
    engine = pg_hook.get_sqlalchemy_engine()  # Получение engine

    data = pd.read_sql('select * from raw.events', engine)  # Чтение таблицы events
    kwargs['ti'].xcom_push(key='events_data', value=data)  # Сохранение в XCom

# Функция для объединения сессий и событий в одну структуру
def merge_data(**kwargs):
    ti = kwargs['ti']  # Получение task instance
    sessions = ti.xcom_pull(key='sessions_data', task_ids='data_extraction.extract_sessions_data')  # Сессии из XCom
    events = ti.xcom_pull(key='events_data', task_ids='data_extraction.extract_events_data')  # События из XCom

    sessions = sessions.values.tolist()  # Преобразование в список списков
    events = events.values.tolist()  # Аналогично для событий

    session_dict = {}  # Словарь для хранения агрегаций по session_id
    for sess in sessions:
        session_id = sess[0]  # Первый столбец — session_id
        session_dict[session_id] = {
            'duration_sec': sess[5],  # Продолжительность сессии
            'event_count': 0,         # Кол-во событий (пока 0)
            'session_date': sess[2]   # Дата сессии
        }

    for event in events:
        session_id = event[1]  # Связь события с session_id
        if session_id in session_dict:
            session_dict[session_id]['event_count'] += 1  # Увеличиваем счётчик событий

    # Подготовка результата в виде списка кортежей
    result = [
        (sess_id, data['duration_sec'], data['event_count'])
        for sess_id, data in session_dict.items()
    ]

    ti.xcom_push(key='joined_data', value=result)  # Сохраняем в XCom

# Функция удаления дубликатов из объединённых данных
def check_duplicates(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='joined_data', task_ids='merge_sessions_data')  # Получаем объединённые данные
    df = pd.DataFrame(data, columns=['session_id', 'duration_sec', 'event_count'])  # Преобразуем в DataFrame
    df = df.drop_duplicates()  # Удаляем дубликаты
    ti.xcom_push(key='cleaned_data', value=df.values.tolist())  # Сохраняем очищенные данные

# Функция для фильтрации строк с ненулевыми статистиками
def check_non_zero_stats(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='cleaned_data', task_ids='data_quality.check_duplicates')  # Получаем очищенные данные
    df = pd.DataFrame(data, columns=['session_id', 'duration_sec', 'event_count'])  # Преобразуем в DataFrame
    df = df[(df['duration_sec'] > 0) & (df['event_count'] > 0)]  # Оставляем только строки с ненулевыми значениями
    ti.xcom_push(key='validated_data', value=df.values.tolist())  # Сохраняем результат

# Функция загрузки финальных данных во временную таблицу
def load_to_temp_table(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='validated_data', task_ids='data_quality.check_non_zero_stats')  # Получаем финальные данные

    hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Подключение к БД
    hook.run("DELETE FROM temp.session_event_stats", autocommit=True)  # Чистим временную таблицу

    for row in data:
        hook.run(f"""
            INSERT INTO temp.session_event_stats VALUES (
                '{row[0]}', '{row[1]}', '{row[2]}'
            )
        """, autocommit=True)  # Вставляем строку за строкой

# Создание DAG
with DAG(
    'extract_and_prepare_data',  # Название DAG
    description='Извлечение данных и подготовка',  # Описание
    schedule_interval='@daily',  # Период запуска
    start_date=datetime(2025, 7, 7),  # Дата начала
    catchup=False,  # Не догонять старые даты
    tags=['raw'],  # Теги для UI
) as dag:

    # Группа задач по извлечению данных
    with TaskGroup('data_extraction') as extraction_group:
        extract_data_sessions_task = PythonOperator(
            task_id='extract_sessions_data',  # Название задачи
            python_callable=extract_session_data,  # Функция для запуска
            provide_context=True  # Передаём контекст с XCom
        )

        extract_data_events_task = PythonOperator(
            task_id='extract_events_data',
            python_callable=extract_events_data,
            provide_context=True
        )

        extract_data_sessions_task >> extract_data_events_task  # Последовательность: сначала sessions, потом events

    # Задача объединения данных
    merge_data_task = PythonOperator(
        task_id='merge_sessions_data',
        python_callable=merge_data,
        provide_context=True
    )

    # Группа задач по проверке качества данных
    with TaskGroup('data_quality', tooltip='Очистка и проверка данных') as quality_group:
        remove_duplicates = PythonOperator(
            task_id='check_duplicates',
            python_callable=check_duplicates,
            provide_context=True
        )

        validate_non_zero = PythonOperator(
            task_id='check_non_zero_stats',
            python_callable=check_non_zero_stats,
            provide_context=True
        )

        remove_duplicates >> validate_non_zero  # Сначала удаление дубликатов, потом фильтрация по нулям

    # Задача загрузки во временную таблицу
    joined_data_task = PythonOperator(
        task_id='joined_data_task',
        python_callable=load_to_temp_table,
        provide_context=True
    )

    # Запуск другого DAG после завершения текущего
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_DAG2',
        trigger_dag_id='agg_sessions',  # Название DAG-а, который будет запущен
        execution_date='{{ execution_date }}',  # Передача текущей execution_date
        wait_for_completion=False,  # Не ждать завершения запущенного DAG
        reset_dag_run=True,  # Сброс предыдущего прогона, если он был
        trigger_rule='all_success'  # Запускать только при успехе всех предыдущих задач
    )

    # Объявление последовательности задач
    extraction_group >> merge_data_task >> quality_group >> joined_data_task >> trigger_dag
