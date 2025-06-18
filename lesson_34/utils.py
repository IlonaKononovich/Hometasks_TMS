# Импортируем модули для работы с датой и временем
from datetime import datetime, timedelta
# Импортируем Faker для генерации фейковых данных
from faker import Faker
# Импортируем random для случайных значений
import random
# Импортируем pandas для работы с табличными данными
import pandas as pd
# Импортируем os для работы с файловой системой
import os
# Импортируем tempfile для получения временной директории ОС
import tempfile
# Импортируем библиотеку для отображения логов
import logging

# Функция для вывода текущего времени — фиксирует момент начала генерации
def time_of_generation():
    time = datetime.now()  # Получаем текущую дату и время
    logging.info(f'Время начала генерации: {time}')


# Функция для генерации Excel файла с фейковыми пользователями
def generate_users_file():
    fake = Faker('ru_RU')  # Инициализируем Faker с русской локализацией
    data = []  # Создаём пустой список для данных

    # Генерируем 20 пользователей с именем, фамилией и городом
    for _ in range(20):
        first_name = fake.first_name()  # Генерируем имя
        last_name = fake.last_name()    # Генерируем фамилию
        city = fake.city()              # Генерируем город
        data.append([first_name, last_name, city])  # Добавляем в список

    # Создаём DataFrame из списка данных с заданными названиями колонок
    df = pd.DataFrame(data, columns=["Имя", "Фамилия", "Город"])

    # Получаем путь к временной директории ОС
    temp_dir = tempfile.gettempdir()
    # Формируем полный путь к файлу users.xlsx в этой директории
    filepath = os.path.join(temp_dir, "users.xlsx")

    # Сохраняем DataFrame в Excel без индекса строк
    df.to_excel(filepath, index=False)

    # Выводим успешное сообщение с путём к файлу
    logging.info(f"[v] Файл users.xlsx успешно создан: {filepath}")


# Функция для генерации текстового файла с продажами
def generate_sales_txt():
    # Список товаров, из которых будут случайно выбираться продажи
    products = [
        "Бананы", "Яблоки", "Молоко", "Хлеб", "Сыр",
        "Шоколад", "Йогурт", "Кофе", "Чай", "Макароны"
    ]

    # Случайное количество продаж от 30 до 80
    num_sales = random.randint(30, 80)

    # Получаем временную директорию
    temp_dir = tempfile.gettempdir()
    # Путь к файлу sales.txt в этой директории
    filepath = os.path.join(temp_dir, "sales.txt")

    # Открываем файл для записи с кодировкой UTF-8
    with open(filepath, "w", encoding="utf-8") as file:
        # Генерируем каждую продажу
        for _ in range(num_sales):
            product = random.choice(products)        # Случайный товар
            quantity = random.randint(1, 100)        # Случайное количество
            price = random.randint(30, 500)          # Случайная цена
            line = f"{product},{quantity}шт,{price}р"  # Форматируем строку
            file.write(line + "\n")                   # Записываем строку в файл

    # Выводим сообщение об успешном создании файла и количестве строк
    logging.info(f"[v] Файл sales.txt успешно создан с {num_sales} продажами: {filepath}")


# Функция для подсчёта строк в файлах и вывода результата
def count_rows():
    # Получаем путь к временной директории
    temp_dir = tempfile.gettempdir()

    # Список кортежей: имя файла и функция для чтения этого файла
    files_info = [
        ("users.xlsx", pd.read_excel),  # Excel читается через pandas
        ("sales.txt", open)             # Текстовый файл читается стандартным open
    ]

    # Проходимся по каждому файлу из списка
    for filename, reader in files_info:
        path = os.path.join(temp_dir, filename)  # Формируем полный путь к файлу

        # В зависимости от типа файла читаем его по-разному
        if filename.endswith(".xlsx"):
            df = reader(path)         # Читаем Excel в DataFrame
            count = len(df)           # Кол-во строк — количество записей
        else:
            with reader(path, "r", encoding="utf-8") as f:
                count = len(f.readlines())  # Считаем количество строк в текстовом файле

        # Выводим результат подсчёта строк
        logging.info(f"[v] Файл {filename} содержит {count} строк.")

if __name__ == "__main__":
    pass