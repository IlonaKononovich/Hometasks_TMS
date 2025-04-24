'''
Домашнее задание 21 (необходимо сделать 2 из 3 задач):
Задача 1. Экспорт расписания рейсов по конкретному маршруту.
Нужно создать функцию на Python, которая выгружает в CSV-файл расписание рейсов между двумя городами (например, Москва и Санкт-Петербург). Функция должна включать:
- Номер рейса
- Время вылета и прилета
- Тип самолета
- Среднюю цену билета
'''

import psycopg2
import pandas as pd

def get_connection():
    # Устанавливаем соединение с базой данных PostgreSQL
    return psycopg2.connect(
        host='localhost',      # Адрес сервера базы данных (localhost — это локальная машина)
        port=5432,             # Порт, на котором работает PostgreSQL (по умолчанию 5432)
        database='demo',       # Название базы данных, к которой подключаемся
        user='postgres',       # Имя пользователя PostgreSQL
        password='postgres'    # Пароль пользователя
    )

def flight_schedule_between_two_cities_to_csv(city_departure:str, city_arrival:str):
    
    # Устанавливаем соединение с базой данных
    conn = get_connection()

    # Создаем курсор для выполнения SQL-запросов
    cursor = conn.cursor()

    # SQL-запрос
    query = """
    select
        %s as city_departure,
        %s as city_arrival,
        f.flight_no,
        f.scheduled_departure,
        f.scheduled_arrival,
        a.model,
        avg(tf.amount)
    from bookings.flights f 
    inner join bookings.ticket_flights tf 
        on tf.flight_id = f.flight_id
    inner join bookings.aircrafts a 
        on a.aircraft_code = f.aircraft_code
    inner join bookings.airports ai1
        on ai1.airport_code  = f.departure_airport
    inner join bookings.airports ai2
        on ai2.airport_code  = f.arrival_airport
    group by f.flight_no, f.scheduled_departure, f.scheduled_arrival, a.model;
    """
    # Выполняем запрос, передавая значения как параметры (безопасно по отношению к SQL-инъекциям)
    cursor.execute(query, (city_departure, city_arrival))

    # Извлекаем все строки результата
    result = cursor.fetchall()

    # Преобразуем результат в DataFrame
    df = pd.DataFrame(result, columns=['city_departure', 'city_arrival', 'flight_no', 'scheduled_departure', 'scheduled_arrival', 'model', 'avg_amount'])

    # Записываем данные в CSV файл
    df.to_csv('/Users/ilonakononovic/PycharmProjects/Hometasks_TMS/lesson_21/flight_schedule_between_two_cities.csv', index=False)

    # Подтверждаем успешный экспорт
    print(f"Данные успешно экспортированы в файл 'flight_schedule_between_two_cities.csv'")

    # Закрываем курсор и соединение с базой данных
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # Пример вызова функции с двумя городами
    flight_schedule_between_two_cities_to_csv('Москва', 'Санкт-Петербург')