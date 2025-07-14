from airflow import DAG  
from airflow.operators.email import EmailOperator 
from datetime import datetime 

# Аргументы по умолчанию для DAG и тасков
default_args = {
    'owner': 'ilona',
    'start_date': datetime(2025, 7, 1),
}

# Создаём DAG с заданными параметрами
with DAG(
    dag_id='send_email_dag', 
    description = 'Отправка письма с помощью EmailOperator',
    default_args=default_args,
    schedule_interval=None,  # Запуск вручную (без расписания)
    catchup=False,
    tags=['operator'],
) as dag:

    # Определяем задачу — отправка письма
    send_email = EmailOperator(
        task_id='send_email_task',
        to='ilona.artyukh@gmail.com',
        subject='Привет от Airflow',  # Тема письма
        html_content='<p> Ура! У тебя получилось это сделать. Ты молодец! <br> Успешно отправлено через Airflow!</p>',  # Содержимое письма в формате HTML
    )

    send_email

