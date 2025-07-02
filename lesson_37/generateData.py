from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from faker import Faker
import os
import random
import pandas as pd


# Инициализация всех параметров и объектов
fake = Faker()
DATA_DIR = '/opt/airflow/dags/input/'
os.makedirs(DATA_DIR, exist_ok=True)

cities = ['Минск', 'Новополоцк', 'Гродно', 'Витебск', 'Гомель', 'Брест', 'Могилёв']
warehouses = {
    "Гродно":'ул. Ожешко, д. 12, Гродно',
    "Витебск":'ул. Лазо, д. 8, Витебск',
    "Минск": 'ул. Карвата, д. 14, Минск',
    "Гомель":'пр-т Ленина, д. 50, Гомель',
}
products = [
    'Электронная книга',
    'Портативный аккумулятор',
    'Наушники Bluetooth',
    'Клавиатура механическая',
    'Мышь беспроводная',
    'Мышка трекбол'
]
delivery_companies = ['EMS Беларусь', 'БелПочта', 'DHL Беларусь', 'NovaPoshta', 'СберЛогистика']

def generate_linked_data(**kwargs):
    users = []
    orders = []
    deliveries = []

    for _ in range(random.randint(5, 10)):
        user_id     = f'U-{random.randint(1000, 9999)}'
        name        = fake.first_name()
        surname     = fake.last_name()
        age         = random.randint(18, 90)
        email       = f"{name.lower()}.{surname.lower()}@{fake.free_email_domain()}"
        phone       = fake.phone_number()
        card_number = fake.credit_card_number(card_type=None)

        users.append({
            'user_id':      user_id,     
            'name':         name,        
            'surname':      surname,     
            'age':          age,       
            'email':        email,    
            'phone':        phone,      
            'card_number':  card_number
        })
# Генерация заказов для пользователя (от 1 до 3 заказов)
        for _ in range(random.randint(1, 3)):
            # Данные заказа
            order_id = random.randint(1000, 9999)  # Уникальный ID заказа
            product = random.choice(products)  # Случайный товар
            quantity = random.randint(1, 5)  # Случайное количество
            price_per_unit = round(random.uniform(10, 300), 2)  # Случайная цена
            total_price = round(quantity * price_per_unit, 2)  # Общая стоимость

            orders.append({
                'order_id': order_id,
                'product': product,
                'quantity': quantity,
                'price_per_unit': price_per_unit,
                'total_price': total_price,
                'card_number': card_number,  # Привязка к карте пользователя
                'user_id': user_id  # Привязка к пользователю
            })

            # Генерация доставки (с вероятностью 90%)
            if random.random() < 0.9:
                delivery_id = f"DLVR-{random.randint(1000, 9999)}"  # ID доставки
                city = random.choice(cities)  # Город доставки
                warehouse_city = random.choice(list(warehouses.keys()))  # Город склада
                warehouse_address = warehouses[warehouse_city]  # Адрес склада

                deliveries.append({
                    'delivery_id': delivery_id,
                    'order_id': order_id,  # Привязка к заказу
                    'product': product,  # Товар
                    'company': random.choice(delivery_companies),  # Служба доставки
                    'cost': round(random.uniform(5, 50), 2),  # Стоимость доставки
                    'courier_name': fake.name(),  # Имя курьера
                    'courier_phone': fake.phone_number(),  # Телефон курьера
                    'start_time': fake.date_between(start_date='-1d', end_date='+1d'),  # Дата отправки
                    'end_time': fake.date_between(start_date='today', end_date='+1d'),  # Дата доставки
                    'city': city,  # Город доставки
                    'warehouse': warehouse_city,  # Город склада
                    'address': warehouse_address  # Адрес склада
                })

    # Сохранение данных в CSV файлы с временной меткой в имени
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Текущая дата-время как строка
    
    # Сохраняем данные пользователей
    pd.DataFrame(users).to_csv(os.path.join(DATA_DIR, f'users_{timestamp}.csv'), index=False)
    
    # Сохраняем данные заказов
    pd.DataFrame(orders).to_csv(os.path.join(DATA_DIR, f'orders_{timestamp}.csv'), index=False)
    
    # Сохраняем данные доставок
    pd.DataFrame(deliveries).to_csv(os.path.join(DATA_DIR, f'deliveries_{timestamp}.csv'), index=False)
    
with DAG(
    'GENERATE_DATA',
    description = 'Генерация данных',
    schedule_interval = timedelta(minutes=5),
    start_date=datetime(2025, 6, 26),
    catchup=False
) as dag:
    
    generate_task = PythonOperator(
        task_id = 'generate_data_task',
        python_callable=generate_linked_data
    )
