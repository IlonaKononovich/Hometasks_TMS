# Используем официальный минималистичный образ Python 3.9 как базовый
FROM python:3.9-slim

# Устанавливаем рабочую директорию внутри контейнера
WORKDIR /app

# Обновляем пакеты и устанавливаем cron, при этом:
# - --no-install-recommends не устанавливает дополнительные, необязательные пакеты
# - очищаем кеш apt после установки, чтобы уменьшить размер образа
RUN apt-get update && \
    apt-get install -y --no-install-recommends cron && \
    rm -rf /var/lib/apt/lists/*

# Создаем символические ссылки на python и pip, если они не существуют в /usr/bin
# Это делается для совместимости со скриптами, которые ожидают их в этих путях
RUN if [ ! -f /usr/bin/python ]; then ln -s /usr/local/bin/python /usr/bin/python; fi && \
    if [ ! -f /usr/bin/pip ]; then ln -s /usr/local/bin/pip /usr/bin/pip; fi

# Устанавливаем Python-библиотеки:
# - pandas: для обработки данных
# - openpyxl: для работы с .xlsx-файлами
RUN pip install pandas openpyxl

# Копируем все файлы из текущей директории (на хосте) в рабочую директорию контейнера
COPY . .

# Делаем скрипт настройки cron исполняемым
RUN chmod +x /app/setup_cron.sh

# Выполняем скрипт настройки cron (создает cron-задачу)
RUN /app/setup_cron.sh

# Запускаем службу cron в фоновом режиме (foreground),
# чтобы контейнер "жил" и не завершался сразу после старта
CMD ["cron", "-f"]
