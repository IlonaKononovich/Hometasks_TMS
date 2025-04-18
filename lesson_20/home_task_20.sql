--Задание 3.1: Анализ задержек рейсов по аэропорту
--Задача: Для указанного аэропорта (по коду аэропорта) вывести статистику задержек.
--Таблица: flights

SELECT 
    f.flight_id AS flight_id,
    ABS(EXTRACT(EPOCH FROM (f.scheduled_departure - f.actual_departure)))::int AS departure_delay_sec,  -- Задержка вылета (в секундах)
    ABS(EXTRACT(EPOCH FROM (f.scheduled_arrival - f.actual_arrival)))::int AS arrival_delay_sec  -- Задержка прилета (в секундах)
FROM bookings.flights f
WHERE f.departure_airport = 'MJZ'  -- Пример, анализ для аэропорта MJZ
order by departure_delay_sec , arrival_delay_sec; -- Сортировка по задержкам


--Задание 3.2: Обернуть в функцию c вводом кода аэропорта
DROP FUNCTION IF EXISTS bookings.airport_delay_statistics(bpchar);

	select 
    	f.flight_id																		as flight_id,
    	abs(extract(epoch from (f.scheduled_departure - f.actual_departure)))::int		as departure_delay_sec, -- Задержка вылета (в секундах)
    	abs(extract(epoch from (f.scheduled_arrival - f.actual_arrival)))::int			as arrival_delay_sec -- Задержка прилета (в секундах)
	from bookings.flights f
	where f.departure_airport = airport_delay_statistics.airport_code; -- Фильтрация по коду аэропорта


create or replace function bookings.airport_delay_statistics(airport_code bpchar) -- Создаем функцию и передаем ей параметр
returns table(flight_id int, departure_delay_sec int, arrival_delay_sec int) -- Указываем, что функция будет возвращать таблицу с тремя столбцами
as $$ 
begin	
	return query -- Указываем, что функция вернет результат выполнения запроса
	select 
    	f.flight_id																		as flight_id,
    	abs(extract(epoch from (f.scheduled_departure - f.actual_departure)))::int		as departure_delay_sec, -- Задержка вылета (в секундах)
    	abs(extract(epoch from (f.scheduled_arrival - f.actual_arrival)))::int			as arrival_delay_sec -- Задержка прилета (в секундах)
	from bookings.flights f
	where f.departure_airport = airport_delay_statistics.airport_code; -- Фильтрация по коду аэропорта
end;
$$ language plpgsql;


select * 
from bookings.airport_delay_statistics('MJZ') -- Вызов функции с кодом аэропорта
order by departure_delay_sec, arrival_delay_sec; -- Сортировка по задержкам




-- Задание 4.1: Найти рейсы с количеством проданных билетов выше среднего по всем рейсам
-- Задача: Найти рейсы, где количество проданных билетов превышает среднее по всем рейсам
-- Таблица: flights

-- создаем CTE для подсчета проданных билетов по каждому рейсу
with tickets_sold as (
    select 
        f.flight_id,  -- Идентификатор рейса
        count(tf.ticket_no) as amount_of_tickets_sold  -- Количество проданных билетов
    from bookings.flights f
    join bookings.ticket_flights tf  -- Соединяем с таблицей билетных рейсов
        on tf.flight_id = f.flight_id  
    group by f.flight_id  -- Группируем по идентификатору рейса
)
-- Основной запрос для вывода рейсов, где количество проданных билетов выше среднего
select    
    flight_id,  
    amount_of_tickets_sold 
from tickets_sold
where amount_of_tickets_sold > (  -- Фильтруем рейсы, где количество проданных билетов больше среднего
    -- Подзапрос
    select avg(amount_of_tickets_sold) 
    from tickets_sold
);

-- Задание 4.2: Функция с параметром минимального процента заполняемости
-- Задача: Создать функцию, которая принимает минимальный процент заполняемости и выводит все рейсы, удовлетворяющие этому проценту
DROP FUNCTION IF EXISTS bookings.flight_fill_percentage(integer);

CREATE OR REPLACE FUNCTION bookings.flight_fill_percentage(percentage int)
RETURNS TABLE(flight_id int, fill_percentage int)  -- Функция возвращает таблицу с двумя столбцами
AS $$ 
BEGIN
    RETURN QUERY  -- Возвращаем результат запроса
	-- Используем CТЕ для подсчета проданных билетов по каждому рейсу
    WITH tickets_sold AS ( 
        SELECT 
            f.flight_id, 
            count(tf.ticket_no) AS amount_of_tickets_sold  
        FROM bookings.flights f
        JOIN bookings.ticket_flights tf
            ON tf.flight_id = f.flight_id  
        GROUP BY f.flight_id  
    ),
    average_tickets AS (  --используем  CTE для вычисления среднего количества проданных билетов по всем рейсам
        SELECT avg(amount_of_tickets_sold)::int AS avg_tickets 
        FROM tickets_sold
    )
    SELECT
        ts.flight_id,  -- Идентификатор рейса
        (ts.amount_of_tickets_sold * 100 / avg.avg_tickets)::int AS fill_percentage  -- Процент заполняемости рейса относительно среднего
    FROM tickets_sold ts, average_tickets avg
    WHERE (ts.amount_of_tickets_sold * 100 / avg.avg_tickets) >= percentage;  -- Фильтруем только те рейсы, у которых заполняемость больше или равна заданному порогу
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
select * 
from bookings.flight_fill_percentage(50);  -- Вводим минимальный процент заполняемости (например 50%);