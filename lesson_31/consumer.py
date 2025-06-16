import json                 # Для работы с JSON — преобразование строк в словари и обратно
import psycopg2              # Для работы с PostgreSQL из Python
from confluent_kafka import Consumer, KafkaException  # Kafka consumer и исключения

# Конфигурация консьюмера Kafka
conf = {
    'bootstrap.servers': 'localhost:9094',   # Адрес Kafka брокера
    'group.id': 'sensor_consumer_group',    # Идентификатор группы потребителей
    'auto.offset.reset': 'earliest'          # Если нет сохранённого оффсета — читать с самого начала. А если указан 'latest' — читать только новые сообщения, которые придут после запуска консьюмера (пропустить старые).
}

consumer = Consumer(conf)                    # Создаём объект Consumer с указанной конфигурацией
consumer.subscribe(['sensor_data'])          # Подписываемся на топик sensor_data

# Подключение к базе данных PostgreSQL
db_conn = psycopg2.connect(
    host='localhost',
    database='testdb',
    user='admin',
    password='secret',
    port='5432'
)

cursor = db_conn.cursor()                    # Создаём курсор для выполнения SQL-запросов

# Функция для вставки данных в таблицу sensor_readings
def insert_into_postgres(data):
    query = """
                INSERT INTO sensor_readings (sensor_id, timestamp, value)
                VALUES (%s, %s, %s)
            """
    cursor.execute(query, (data['sensor_id'], data['timestamp'], data['value']))  # Подставляем данные
    db_conn.commit()    # Сохраняем изменения в базе (без commit данные не сохранятся)

try:
    print("[v] Консъюмер начал работать..")
    while True:
        msg = consumer.poll(timeout=1.0)    # Ждём сообщение из Kafka, таймаут 1 секунда
        
        if msg is None:    # Если сообщений нет — просто продолжаем ждать
            continue

        try:
            # Получаем данные из сообщения: байты -> строка -> словарь
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[v] Сообщение получено: {data}')
            insert_into_postgres(data)      # Вставляем данные в базу
        except Exception as e:              # Если ошибка с сообщением или вставкой
            print(f'[x] Ошибка обработки сообщения: {e}')

except KeyboardInterrupt:                   # Если прервать программу Ctrl+C
    print("[x] Консъюмер остановлен")

finally:
    cursor.close()     # Закрываем курсор (чистим ресурсы)
    db_conn.close()    # Закрываем соединение с базой
    consumer.close()   # Закрываем консьюмер Kafka
