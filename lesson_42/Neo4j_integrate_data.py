# Импорты из Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Импорты для работы с Neo4j, датой и переменными
from neo4j import GraphDatabase
from datetime import datetime, timedelta
from airflow.models import Variable

# Получаем параметры подключения к Neo4j из Airflow Variables
uri = Variable.get("NEO4J_URI")
user = Variable.get("NEO4J_USER")
password = Variable.get("NEO4J_PASSWORD")

# Аргументы DAG-а по умолчанию
default_args = {
    'depends_on_past': False,             # Не зависит от предыдущих запусков
    'start_date': datetime(2025, 7, 13),  # Дата начала запуска DAG-а
}

# Основная функция переноса данных из Neo4j в Postgres
def transfer_from_neo4j_to_postgres():
    # Устанавливаем соединение с Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Устанавливаем фильтр по времени — берём данные за последние 5 минут
    five_min_ago = datetime.now() - timedelta(minutes=5)
    five_min_ago_str = five_min_ago.isoformat()

    # Cypher-запрос к Neo4j: извлекаем клиентов и их транзакции за последние 5 минут
    query = """
    MATCH (c:Customer)-[:MADE]->(t:Transaction)
    WHERE t.timestamp >= datetime($time_threshold)
    RETURN 
        t.transaction_id AS transaction_id,
        c.customer_id AS customer_id,
        c.name AS customer_name,
        t.amount AS amount,
        t.currency AS currency,
        t.type AS transaction_type,
        t.status AS status,
        t.timestamp AS transaction_timestamp,
        c.email AS customer_email,
        c.created_at AS customer_created_at
    """

    # Выполняем запрос и получаем результат
    with driver.session() as session:
        result = session.run(query, time_threshold=five_min_ago_str)
        data = [row.data() for row in result]

    driver.close()  # Закрываем соединение с Neo4j

    if data:
        # Подключаемся к Postgres через PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Вставляем каждую запись в таблицу Postgres
        for record in data:
            cursor.execute('''
                INSERT INTO raw.bank_transactions_neo4j VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING  -- Игнорируем дубликаты
            ''', (
                record['transaction_id'],
                record['customer_id'],
                record['customer_name'],
                float(record.get('amount', 0)),
                record['currency'],
                record['transaction_type'],
                record['status'],
                datetime.fromisoformat(record['transaction_timestamp']),
                record['customer_email'],
                datetime.fromisoformat(record['customer_created_at']),
                datetime.now()  # Время загрузки
            ))

        conn.commit()        # Сохраняем изменения
        cursor.close()       # Закрываем курсор
        conn.close()         # Закрываем соединение

# Определение DAG-а
with DAG(
    'NEO4J_integrate_data',                 # Уникальный ID DAG-а
    default_args=default_args,             # Параметры по умолчанию
    schedule_interval='*/5 * * * *',       # Запуск каждые 5 минут
    catchup=False                          # Не запускать пропущенные интервалы
) as dag:
    
    # Определение задачи переноса данных
    transfer_from_neo4j = PythonOperator(
        task_id='transfer_from_neo4j',
        python_callable=transfer_from_neo4j_to_postgres
    )

    # Задача в DAG-е (одиночная)
    transfer_from_neo4j
