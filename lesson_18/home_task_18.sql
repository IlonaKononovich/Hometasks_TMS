-- Задача 1: Вывести аэропорты, из которых выполняется менее 50 рейсов
select 
    count(f.flight_no) as flights_ammount,   -- Подсчитываем количество рейсов для каждого аэропорта
    a.airport_name   						 -- Имя аэропорта
from bookings.flights f 
inner join bookings.airports a 
    on a.airport_code = f.departure_airport  -- Соединяем таблицы рейсов и аэропортов по коду аэропорта вылета
group by a.airport_name 					 -- Группируем по имени аэропорта
having count(f.flight_no) < 50  			 -- Отбираем только те аэропорты, где выполняется менее 50 рейсов
order by flights_ammount desc 				 -- Сортируем по количеству рейсов в убывающем порядке
;


-- Задача 2: Вывести среднюю стоимость билетов для каждого маршрута (город вылета - город прилета)
select
    round(avg(tf.amount), 0) 				as average_ticket_amount,  -- Находим среднюю стоимость билетов и округляем до 0 знаков после запятой
    a1.city								    as departure_city,  	   -- Город вылета
    a2.city 								as arrival_city    		   -- Город прилета
from bookings.flights f
inner join bookings.ticket_flights tf 						 		   -- Соединяем таблицу билетов с таблицей рейсов по flight_id
    on tf.flight_id = f.flight_id
inner join bookings.airports a1  									   -- Соединяем таблицу аэропортов для получения города вылета
    on a1.airport_code = f.departure_airport
inner join bookings.airports a2  									   -- Соединяем таблицу аэропортов для получения города прилета
    on a2.airport_code = f.arrival_airport
group by a1.city, a2.city  											   -- Группируем по городам вылета и прилета
order by average_ticket_amount desc;  								   -- Сортируем по средней стоимости билета в убывающем порядке
;


-- Задача 3: Вывести топ-5 самых загруженных маршрутов (по количеству проданных билетов)
select
    count(tf.ticket_no) 					as amount_of_tickets_sold, -- Подсчитываем количество проданных билетов для каждого маршрута
    a1.city 								as departure_city, 		  
    a2.city 								as arrival_city    		  
from bookings.flights f
inner join bookings.ticket_flights tf 								   
    on tf.flight_id = f.flight_id
inner join bookings.airports a1  									   
    on a1.airport_code = f.departure_airport
inner join bookings.airports a2 									  
    on a2.airport_code = f.arrival_airport
group by a1.city, a2.city 											   
order by amount_of_tickets_sold desc 								   -- Сортируем по количеству проданных билетов в убывающем порядке
limit 5; 															   -- Ограничиваем выборку 5 самыми загруженными маршрутами
;


--- Задача 4: Найти пары рейсов, вылетающих из одного аэропорта в течение 1 часа
--Подсказка: (f1.scheduled_departure - f2.scheduled_departure))) <= 3600

select
    f1.flight_no,  -- Номер рейса из первой таблицы
    f2.flight_no   -- Номер рейса из второй таблицы
from bookings.flights f1
-- Присоединяем таблицу рейсов к самой себе
inner join bookings.flights f2 
    on f1.flight_id != f2.flight_id  -- Исключаем самоприсоединение рейса к себе
where 
    abs(extract(EPOCH from (f1.scheduled_departure - f2.scheduled_departure))) <= 3600;
    -- Разница в вылетах двух рейсов не более 1 часа (3600 секунд)
