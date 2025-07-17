from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from neo4j import GraphDatabase
from datetime import datetime, timedelta

from airflow.models import Variable

uri = Variable.get("NEO4J_URI")
user = Variable.get("NEO4J_USER")
password = Variable.get("NEO4J_PASSWORD")

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 13),
}

def transfer_from_neo4j_to_postgres():
    driver = GraphDatabase.driver(uri, auth=(user, password))

    five_min_ago = datetime.now() - timedelta(minutes=5)
    five_min_ago_str = five_min_ago.isoformat()

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

    with driver.session() as session:
        result = session.run(query, time_threshold=five_min_ago_str)
        data = [row.data() for row in result]

    driver.close()

    if data:
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for record in data:
            cursor.execute('''
                INSERT INTO raw.bank_transactions_neo4j VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
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
                datetime.now()
            ))

        conn.commit()
        cursor.close()
        conn.close()

with DAG(
    'NEO4J_integrate_data',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:
    transfer_from_neo4j = PythonOperator(
        task_id='transfer_from_neo4j',
        python_callable=transfer_from_neo4j_to_postgres
    )

    transfer_from_neo4j
