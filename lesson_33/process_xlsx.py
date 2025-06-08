# Импортируем необходимые библиотеки
import pandas as pd  # библиотека для работы с таблицами (Excel, CSV и др.)
import sys  # модуль для работы с аргументами командной строки
import os  # модуль для работы с файловой системой
from datetime import datetime  # для генерации текущего времени (например, метки времени в названии файла)

# Функция для обработки Excel-файла
def proccess_xlsx_file(input_file: str):
    # Читаем Excel-файл в DataFrame
    df = pd.read_excel(input_file)

    # Удаляем дубликаты строк (оставляя только уникальные)
    unique_df = df.drop_duplicates()

    # Разделяем имя и расширение исходного файла
    file_name, file_ext = os.path.splitext(input_file)
    
    # Генерируем метку времени в формате "ГГГГММДД_ЧЧММСС"
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Формируем имя выходного файла: оригинальное имя + "_unique_" + метка времени
    output_file = f"{file_name}_unique_{timestamp}{file_ext}"

    # Сохраняем DataFrame без дубликатов в новый Excel-файл
    unique_df.to_excel(output_file, index=False)

    # Выводим сообщение об успешном сохранении
    print(f'[v] Уникальные данные сохранены в: {output_file}')


# Блок, который запускается, если скрипт запущен напрямую (а не импортирован как модуль)
if __name__ == "__main__":
    # Получаем имя входного файла из аргументов командной строки
    input_file_name = sys.argv[1]

    # Проверяем, существует ли указанный файл
    if not os.path.exists(input_file_name):
        print(f"Файл {input_file_name} не найден")
        sys.exit(1)  # Завершаем программу с кодом ошибки 1

    # Запускаем функцию обработки Excel-файла
    proccess_xlsx_file(input_file_name)
