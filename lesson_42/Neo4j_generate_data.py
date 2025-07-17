from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker
from neo4j import GraphDatabase
from datetime import datetime
import random
from airflow.models import Variable

uri = Variable.get("NEO4J_URI")
user = Variable.get("NEO4J_USER")
password = Variable.get("NEO4J_PASSWORD")


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 13),
}

def generate_bank_data():
    fake = Faker()
    data = []

    for _ in range(50):
        customer = {
            "customer_id": fake.uuid4(),
            "name": fake.name(),
            "email": fake.email(),
            "created_at": datetime.now()
        }

        transactions = []
        for _ in range(random.randint(1, 3)):
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


def insert_to_neo4j(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='generate_data')


    driver = GraphDatabase.driver(uri, auth=(user, password))

    def insert_customer_data(tx, customer, transactions):
        tx.run(
            """
            MERGE (c:Customer {customer_id: $customer_id})
            SET c.name = $name, c.email = $email, c.created_at = $created_at
            """,
            **customer
        )

        for txn in transactions:
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

    with driver.session() as session:
        for entry in data:
            session.write_transaction(insert_customer_data, entry["customer"], entry["transactions"])

    driver.close()


with DAG(
        'NEO4J_generate_bank_data',
        default_args=default_args,
        schedule_interval='*/5 * * * *',
        catchup=False
) as dag:
    generate_data = PythonOperator(
        task_id='generate_data',
        python_callable=generate_bank_data
    )

    load_to_neo4j = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=insert_to_neo4j
    )

    generate_data >> load_to_neo4j
