#!/bin/bash
# Указываем, что скрипт должен выполняться интерпретатором bash

# Создаем (или обновляем время последнего изменения) лог-файл для вывода задач cron
touch /var/log/cron.log

# Добавляем новую cron-задачу:
# * * * * * — означает запуск каждую минуту
# root — от имени пользователя root
# cd /app && ... — сначала переходим в каталог /app, затем запускаем Python-скрипт
# >> /var/log/cron.log 2>&1 — перенаправляем стандартный вывод и ошибки в лог-файл
echo "* * * * * root cd /app && /usr/local/bin/python /app/process_xlsx.py /app/data/input_file.xlsx >> /var/log/cron.log 2>&1" > /etc/cron.d/xlsx-job

# Устанавливаем права доступа к cron-файлу:
# 0644 = владелец может читать и писать, остальные — только читать
chmod 0644 /etc/cron.d/xlsx-job

# Регистрируем созданную cron-задачу в crontab
crontab /etc/cron.d/xlsx-job
