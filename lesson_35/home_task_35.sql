-- Создаём таблицу raw.users с информацией о пользователях
create table raw.users(
    name text,                      -- Имя пользователя (текст)
    surname text,                   -- Фамилия пользователя (текст)
    city text,                      -- Город пользователя (текст)
    age integer,                    -- Возраст пользователя (целое число)
    card_number text primary key    -- Номер карты (текст), уникальный идентификатор (первичный ключ)
);

-- Выбираем все данные из таблицы raw.users
select * from raw.users;

-- Создаём таблицу raw.orders с данными о заказах
create table raw.orders(
    id int primary key,            -- Уникальный идентификатор заказа (целое число, первичный ключ)
    name text,                     -- Название заказа или товара (текст)
    count int,                     -- Количество товаров (целое число)
    price decimal,                 -- Цена за единицу товара (десятичное число)
    total_cost decimal,            -- Общая стоимость заказа (десятичное число)
    card_number text               -- Номер карты пользователя, сделавшего заказ (текст), внешний ключ к raw.users.card_number (неявно)
);

-- Выбираем все данные из таблицы raw.orders
select * from raw.orders;

-- Создаём таблицу raw.deliveries_data с информацией о доставках
CREATE TABLE raw.deliveries_data (
    delivery_number    TEXT PRIMARY KEY,    -- Уникальный номер доставки (текст, первичный ключ)
    order_id           INTEGER,             -- Идентификатор заказа (целое число), внешний ключ к raw.orders.id (неявно)
    product_array      TEXT,                -- Список продуктов в доставке (текст, возможно JSON или массив в текстовом формате)
    delivery_company   TEXT,                -- Компания, осуществляющая доставку (текст)
    delivery_cost      decimal,             -- Стоимость доставки (десятичное число)
    courier_name       TEXT,                -- Имя курьера (текст)
    courier_phone      TEXT,                -- Телефон курьера (текст)
    start_time         TIMESTAMP,           -- Время начала доставки (метка времени)
    end_time           TIMESTAMP,           -- Время окончания доставки (метка времени)
    delivery_city      TEXT,                -- Город доставки (текст)
    warehouse_from     TEXT,                -- Склад отправления (текст)
    warehouse_address  TEXT                 -- Адрес склада (текст)
);

-- Выбираем все данные из таблицы raw.deliveries_data
select * from raw.deliveries_data;


-- Создаём таблицу user_orders_summary в схеме data_mart для сводной информации по пользователям и сумме их заказов
CREATE TABLE IF NOT EXISTS data_mart.user_orders_summary (
    name TEXT,                        -- Имя пользователя (текст)
    card_number TEXT PRIMARY KEY,     -- Номер карты пользователя (текст, первичный ключ)
    total_order_sum DECIMAL           -- Общая сумма всех заказов пользователя (десятичное число)
);


-- Выбираем все данные из сводной таблицы user_orders_summary
select * from data_mart.user_orders_summary;
