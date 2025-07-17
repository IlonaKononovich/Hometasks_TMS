# Импорт модулей и библиотек
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker  # Библиотека для генерации фейковых данных
from neo4j import GraphDatabase  # Драйвер для подключения к Neo4j
from datetime import datetime
import random
from airflow.models import Variable  # Для получения секретов из Airflow Variables

# Получаем параметры подключения к Neo4j из Airflow Variables
uri = Variable.get("NEO4J_URI")
user = Variable.get("NEO4J_USER")
password = Variable.get("NEO4J_PASSWORD")

# Параметры DAG-а по умолчанию
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 13),
}

# Генерация банковских данных: 50 клиентов, у каждого от 1 до 3 транзакций
def generate_bank_data():
    fake = Faker()
    data = []

    for _ in range(50):
        # Случайный клиент
        customer = {
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "created_at": datetime.now()
        }

        transactions = []
        for _ in range(random.randint(1, 3)):
            # Случайная транзакция
            transaction = {
                "transaction_id": fake.uuid4(),
                "amount": round(random.uniform(10, 10000), 2),
                "currency": random.choice(["USD", "EUR", "RUB"]),
                "type": random.choice(["deposit", "withdrawal", "transfer"]),
                "status": random.choice(["completed", "pending", "failed"]),
                "timestamp": fake.date_time_this_year()
            }
            transactions.append(transaction)

        data.append({"customer": customer, "transactions": transactions})

    return data

# Загрузка сгенерированных данных в Neo4j
def insert_to_neo4j(**kwargs):
    # Получаем данные из предыдущего таска через XCom
    data = kwargs['ti'].xcom_pull(task_ids='generate_data')

    # Создаём драйвер Neo4j
    driver = GraphDatabase.driver(uri, auth=(user, password))

    # Вставка клиента и связанных транзакций
    def insert_customer_data(tx, customer, transactions):
        # MERGE клиента
        tx.run(
            """
            MERGE (c:Customer {customer_id: $customer_id})
            SET c.name = $name, c.email = $email, c.created_at = $created_at
            """,
            **customer
        )

        for txn in transactions:
            # MERGE транзакции и связываем с клиентом отношением MADE
            tx.run(
                """
                MERGE (t:Transaction {transaction_id: $transaction_id})
                SET t.amount = $amount,
                    t.currency = $currency,
                    t.type = $type,
                    t.status = $status,
                    t.timestamp = $timestamp

                MERGE (c:Customer {customer_id: $customer_id})
                MERGE (c)-[:MADE]->(t)
                """,
                **txn,
                customer_id=customer["customer_id"]
            )

    # Записываем в базу данных по каждому клиенту
    with driver.session() as session:
        for entry in data:
            session.write_transaction(insert_customer_data, entry["customer"], entry["transactions"])

    # Закрываем соединение
    driver.close()

# Определение DAG-а
with DAG(
        'NEO4J_generate_bank_data',  # Уникальный идентификатор DAG-а
        default_args=default_args,
        schedule_interval='*/5 * * * *',  # Запуск каждые 5 минут
        catchup=False  # Не запускать пропущенные интервалы
) as dag:
    
    # Задача генерации данных
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_bank_data
    )

    # Задача загрузки данных в Neo4j
    load_to_neo4j = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=insert_to_neo4j
    )

    # Устанавливаем последовательность: сначала генерация, потом загрузка
    generate_data >> load_to_neo4j
