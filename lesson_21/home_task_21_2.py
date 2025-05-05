'''
Задача 2. Массовое обновление статусов рейсов
Создать функцию для пакетного обновления статусов рейсов (например, "Задержан" или "Отменен"). Функция должна:
- Принимать список рейсов и их новых статусов
- Подтверждать количество обновленных записей
- Обрабатывать ошибки (например, несуществующие рейсы)
Пример входных данных:
updates = [
    {"flight_id": 123, "new_status": "Delayed"},
    {"flight_id": 456, "new_status": "Cancelled"}
]
update_flights_status(updates)
'''

import psycopg2

def get_connection():
    # Устанавливаем соединение с базой данных PostgreSQL
    return psycopg2.connect(
        host='localhost',      # Адрес сервера базы данных (localhost — это локальная машина)
        port=5432,             # Порт, на котором работает PostgreSQL (по умолчанию 5432)
        database='demo',       # Название базы данных, к которой подключаемся
        user='postgres',       # Имя пользователя PostgreSQL
        password='postgres'    # Пароль пользователя
    )

def update_flights_status(updates):
    # Устанавливаем соединение с базой данных
    conn = get_connection()
    # Создаем курсор
    cursor = conn.cursor()

    # Счетчик для подсчета успешных обновлений
    updated_count = 0

    for update in updates:
        try:
            # SQL-запрос для обновления статуса рейса
            query = """
            UPDATE bookings.flights
            SET status = %s
            WHERE flight_id = %s;
            """
            
            # Выполняем запрос для каждого рейса
            cursor.execute(query, (update["new_status"], update["flight_id"]))
            
            # Проверяем, обновился ли хотя бы один рейс
            if cursor.rowcount > 0:
                updated_count += 1

        except Exception as e:
            # Обрабатываем возможные ошибки (например, неверный flight_id)
            print(f"Ошибка при обновлении рейса с ID {update['flight_id']}: {e}")

    # Подтверждаем изменения
    conn.commit()

    # Подтверждаем количество обновленных записей
    print(f"Количество обновленных записей: {updated_count}")

    # Закрываем курсор и соединение с базой данных
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Пример входных данных
    updates = [
        {"flight_id": 123, "new_status": "Delayed"},
        {"flight_id": 456, "new_status": "Cancelled"}
    ]
    
    # Вызов функции для обновления статусов
    update_flights_status(updates)
