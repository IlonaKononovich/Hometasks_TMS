--Задача 1 
--Вам нужно проанализировать данные о продажах билетов, чтобы получить статистику в следующих разрезах:
--- По классам обслуживания (fare_conditions)
--- По месяцам вылета
--- По аэропортам вылета
--- По комбинациям: класс + месяц, класс + аэропорт, месяц + аэропорт
--- Общие итоги
--
--Используемые таблицы:
--- ticket_flights (информация о билетах)
--- flights (информация о рейсах)
--- airports (информация об аэропортах)


select 	
	tf.fare_conditions,                       
	date_trunc('month', f.scheduled_departure) 	as month_departure,  -- месяц вылета (с округлением до первого дня)
	a.airport_name,                          
	count(*) 									as flight_count                 
from bookings.flights f                      
join bookings.ticket_flights tf                
	on tf.flight_id = f.flight_id
join bookings.airports a                      
	on a.airport_code = f.departure_airport
-- GROUPING SETS для нескольких вариантов группировки в одном запросе
group by grouping sets (
	(tf.fare_conditions),                    -- группировка по классу
    (month_departure),                       -- группировка по месяцу
    (a.airport_name),                        -- группировка по аэропорту
    (tf.fare_conditions, month_departure),   -- группировка по классу и месяцу
	(tf.fare_conditions, a.airport_name),    -- группировка по классу и аэропорту
	(month_departure, a.airport_name),       -- группировка по месяцу и аэропорту
	()                                       -- общий итог (без группировки)
)
-- сортировка результатов
order by month_departure, a.airport_name, tf.fare_conditions;





--Задача 2 для самостоятельного задания: 
--Рейсы с задержкой больше средней (CTE + подзапрос)
--Найдите рейсы, задержка которых превышает среднюю задержку по всем рейсам.
--Используемые таблицы:
--- flights


-- создаем CTE
with delay_amount as (
    select 
        flight_id,  -- выбираем идентификатор рейса
        -- вычисляем задержку рейса (разница между запланированным и фактическим временем вылета в секундах)
        extract(EPOCH from (scheduled_departure - actual_departure)) as delay  
    from bookings.flights
    -- добавляем условие для исключения записей с NULL в actual_departure
    where actual_departure IS NOT NULL
)
-- основной запрос для получения рейсов с задержкой больше средней
select 
    flight_id,  -- идентификатор рейса
    delay  -- задержка рейса в секундах
from delay_amount  -- из CTE "delay_amount"
where delay > (  
    -- подзапрос для вычисления средней задержки по всем рейсам
    select avg(delay) 
    from delay_amount
)
order by delay, flight_id
;



--Задача 3 для самостоятельного задания:
--Создайте представление, которое содержит все рейсы, вылетающие из Москвы.


-- создаем представление для рейсов, вылетающих из Москвы
create view bookings.flights_from_Moscow as
select
    a.city,  
    f.flight_no 
from bookings.flights f
join bookings.airports a
    on f.departure_airport = a.airport_code  -- соединяем рейсы с аэропортами по коду аэропорта вылета
where a.city = 'Москва';  -- фильтруем по городу "Москва"

-- делаем запрос из созданного представления
select * from bookings.flights_from_Moscow



-- Тема "Временные таблицы"
--Временная таблица — это таблица, которая существует только на время сессии (или до закрытия соединения с БД).
--После этого она удаляется автоматически. Это удобно, если нужно временно сохранить или обработать данные.

-- 1. Создание временной таблицы
create temp table temp_users (
    id serial primary key,
    имя text,
    возраст int
);
-- или
create temporary table 

-- 2. Заполнение данными
-- a). Вручную
insert into temp_users (имя, возраст)
values ('Ирина', 25), ('Михаил', 30);

-- б). Из другой таблицы
insert into temp_users (имя, возраст)
select имя, возраст from users
where возраст > 18;


-- 3. JOIN
-- Временная таблица работает как обычная — её можно соединять, фильтровать, группировать
create table orders (
    id SERIAL primary key ,
    user_id int,
    сумма numeric
);

select temp_users.имя, temp_users.возраст, orders.сумма
from temp_users
inner join orders on temp_users.id = orders.user_id;

-- 4. Удаление вручную (Но если закрыть сессию или перезапустишь подключение, временная таблица исчезнет сама)
DROP TABLE temp_users;


-- Главное отличие между временной таблицей, CTE и подзапросом:
-- Временная таблица: живёт дольше - удобно для многократного использования.
-- CTE и подзапрос: живут один раз - идеально для коротких операций.

