from airflow.decorators import dag, task  # Импорт декораторов для создания DAG и задач
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Хук для подключения к Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Оператор для выполнения SQL в Postgres
from datetime import datetime  # Импорт для задания даты старта DAG
import pandas as pd  # Импорт pandas для работы с таблицами и данными
import logging  # Импорт модуля логирования
import os  # Импорт модуля для работы с файловой системой
from models import User, Card  # Импорт моделей для валидации данных

logger = logging.getLogger(__name__)  # Создание логгера для текущего модуля

@dag(  # Определение DAG с помощью декоратора
    dag_id='build_dds_layer_postgres',  # Уникальный идентификатор DAG
    start_date=datetime(2025, 6, 28),  # Дата начала выполнения DAG
    schedule_interval='0 19 * * *',  # Расписание запуска — каждый день в 19:00
    tags=['dds'],  # Теги для группировки DAG в интерфейсе Airflow
    description='DAG для загрузки данных из raw в dds с нормализацией',  # Описание DAG
    catchup=False  # Отключение прогонки DAG за пропущенные даты
)
def build_dds_layer():  # Определение функции DAG

    @task()  # Декоратор для задачи извлечения данных
    def extract_raw_data(table_name: str) -> pd.DataFrame:  # Задача принимает имя таблицы и возвращает DataFrame
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Создание хука с заданным подключением
        try:
            conn = pg_hook.get_conn()  # Получение соединения с базой данных
            df = pd.read_sql(f"SELECT * FROM raw.{table_name}", conn)  # Чтение всей таблицы raw.<table_name> в DataFrame
            logger.info(f"Извлечено {df.shape[0]} строк из raw.{table_name}")  # Логируем количество строк
            return df  # Возвращаем DataFrame с данными
        except Exception as e:  # Если возникает ошибка
            logger.error(f"Ошибка при извлечении raw.{table_name}: {e}")  # Логируем ошибку
            return pd.DataFrame()  # Возвращаем пустой DataFrame при ошибке

    @task()  # Декоратор для задачи трансформации и загрузки пользователей и карт
    def transform_and_load_users(data_df: pd.DataFrame):  # Принимает DataFrame с сырыми данными
        if data_df.empty:  # Если DataFrame пустой
            logger.info("Пустой DataFrame пользователей — пропускаем")  # Логируем и выходим
            return

        users_df = data_df[['user_id', 'name', 'surname', 'age', 'email', 'phone']].drop_duplicates()  # Выбираем колонки пользователей и убираем дубликаты

        valid_users = []  # Список для валидных пользователей
        for record in users_df.to_dict(orient='records'):  # Перебираем пользователей как словари
            try:
                user = User(**record)  # Пытаемся создать валидную модель пользователя
                valid_users.append(user.model_dump())  # Если успешно — добавляем сериализованные данные в список
            except Exception as e:  # Если ошибка валидации
                logger.error(f"Ошибка валидации пользователя {record}: {e}")  # Логируем ошибку

        users_valid_df = pd.DataFrame(valid_users)  # Создаем DataFrame из валидных пользователей
        if users_valid_df.empty:  # Если валидных пользователей нет
            logger.warning("Нет валидных пользователей")  # Логируем предупреждение
            return

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Создаем хук для подключения
        engine = pg_hook.get_sqlalchemy_engine()  # Получаем sqlalchemy engine для записи в БД

        existing_users = pd.read_sql('SELECT user_id FROM dds.users', engine)  # Загружаем существующих пользователей из dds
        new_users = users_valid_df[~users_valid_df['user_id'].isin(existing_users['user_id'])]  # Отбираем новых пользователей, которых еще нет в dds

        if not new_users.empty:  # Если есть новые пользователи
            new_users.to_sql('users', engine, schema='dds', if_exists='append', index=False)  # Добавляем их в dds.users
            logger.info(f"Добавлено новых пользователей: {new_users.shape[0]}")  # Логируем количество добавленных
        else:
            logger.info("Новых пользователей нет")  # Логируем, что новых пользователей нет

        cards_df = data_df[['card_number', 'user_id']].drop_duplicates()  # Аналогично выбираем карты и убираем дубликаты

        valid_cards = []  # Список для валидных карт
        for record in cards_df.to_dict(orient='records'):  # Перебираем карты по одной
            try:
                card = Card(**record)  # Пытаемся создать валидную модель карты
                valid_cards.append(card.model_dump())  # Добавляем валидные карты
            except Exception as e:  # При ошибке валидации
                logger.error(f"Ошибка валидации карты {record}: {e}")  # Логируем ошибку

        cards_valid_df = pd.DataFrame(valid_cards)  # Создаем DataFrame валидных карт
        if cards_valid_df.empty:  # Если нет валидных карт
            logger.warning("Нет валидных карт")  # Логируем предупреждение
            return

        existing_cards = pd.read_sql("SELECT card_number FROM dds.cards", engine)  # Загружаем существующие карты из dds
        new_cards = cards_valid_df[~cards_valid_df['card_number'].isin(existing_cards['card_number'])]  # Выбираем новые карты

        if not new_cards.empty:  # Если есть новые карты
            new_cards.to_sql('cards', engine, schema='dds', if_exists='append', index=False)  # Добавляем их в dds.cards
            logger.info(f"Добавлено новых карт: {new_cards.shape[0]}")  # Логируем количество добавленных карт
        else:
            logger.info("Новых карт нет")  # Логируем отсутствие новых карт

    @task()  # Декоратор для задачи трансформации и загрузки заказов
    def transform_and_load_orders(orders_df: pd.DataFrame):  # Принимает DataFrame с сырыми заказами
        if orders_df.empty:  # Если данные пустые
            logger.info("Пустой DataFrame заказов — пропускаем")  # Логируем и выходим
            return

        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')  # Хук подключения
        engine = pg_hook.get_sqlalchemy_engine()  # Получение sqlalchemy engine

        orders_df = orders_df.drop_duplicates(subset=['order_id'])  # Убираем дубликаты заказов по order_id

        products_df = orders_df[['product']].drop_duplicates().rename(columns={'product': 'product_name'})  # Выделяем уникальные продукты, переименовываем колонку
        existing_products = pd.read_sql("SELECT product_name FROM dds.products", engine)  # Загружаем продукты из dds
        new_products = products_df[~products_df['product_name'].isin(existing_products['product_name'])]  # Отбираем новые продукты

        if not new_products.empty:  # Если есть новые продукты
            new_products.to_sql('products', engine, schema='dds', if_exists='append', index=False)  # Добавляем их в dds.products
            logger.info(f"Добавлено новых продуктов: {new_products.shape[0]}")  # Логируем количество

        all_products = pd.read_sql("SELECT product_id, product_name FROM dds.products", engine)  # Загружаем все продукты с id
        orders_df = orders_df.merge(all_products, left_on='product', right_on='product_name', how='left')  # Добавляем product_id к заказам

        dds_orders = orders_df[['order_id', 'quantity', 'price_per_unit', 'total_price',
                                'card_number', 'user_id', 'product_id']]  # Выбираем необходимые колонки для dds.orders

        dds_orders = dds_orders.drop_duplicates(subset=['order_id'])  # Дополнительно убираем дубликаты заказов

        existing_orders = pd.read_sql("SELECT order_id FROM dds.orders", engine)  # Загружаем существующие заказы
        new_orders = dds_orders[~dds_orders['order_id'].isin(existing_orders['order_id'])]  # Отбираем новые заказы

        if not new_orders.empty:  # Если есть новые заказы
            new_orders.to_sql('orders', engine, schema='dds', if_exists='append', index=False)  # Добавляем их в dds.orders
            logger.info(f"Добавлено новых заказов: {new_orders.shape[0]}")  # Логируем
        else:
            logger.info("Новых заказов нет")  # Логируем отсутствие новых заказов

    SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql', 'dds')  # Путь к папке с SQL скриптами создания таблиц
    table_creation_order = [  # Порядок создания таблиц для нормальной последовательности зависимостей
        'users', 'products', 'delivery_companies', 'couriers',
        'warehouses', 'orders', 'deliveries', 'cards'
    ]

    create_tasks = []  # Список задач создания таблиц
    for table in table_creation_order:  # Для каждой таблицы из списка
        sql_path = os.path.join(SQL_DIR, f'create_{table}.sql')  # Формируем путь к SQL файлу
        with open(sql_path, 'r') as f:  # Открываем файл
            sql = f.read()  # Читаем SQL запрос создания таблицы
        create_task = PostgresOperator(  # Создаем задачу PostgresOperator
            task_id=f'create_table_{table}',  # Идентификатор задачи
            postgres_conn_id='my_postgres_conn',  # Подключение к БД
            sql=sql  # SQL код для выполнения
        )
        create_tasks.append(create_task)  # Добавляем задачу в список

    for i in range(len(create_tasks) - 1):  # Проходим по задачам создания таблиц для последовательного запуска
        create_tasks[i] >> create_tasks[i + 1]  # Устанавливаем зависимость между задачами (последовательность)

    users_raw = extract_raw_data('users')  # Задача извлечения данных пользователей из raw
    orders_raw = extract_raw_data('orders')  # Задача извлечения данных заказов из raw

    users_load = transform_and_load_users(users_raw)  # Задача обработки и загрузки пользователей в dds
    orders_load = transform_and_load_orders(orders_raw)  # Задача обработки и загрузки заказов в dds

    create_tasks[-1] >> [users_load, orders_load]  # Задачи загрузки данных выполняются после создания всех таблиц

dag_instance = build_dds_layer()  # Инициализация DAG
