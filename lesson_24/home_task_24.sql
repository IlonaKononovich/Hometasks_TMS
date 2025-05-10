--Задача 1: Анализ частоты полетов пассажиров
--Определить топ-10 пассажиров, которые чаще всего летают.
--Провести оптимизацию скрипта по необходимости

explain analyze
select 
	passenger_name,
	count(ticket_no) as amount_flights
from bookings.tickets
group by passenger_name
order by amount_flights desc
limit 10;

--explain analyze
--Limit  (cost=12004.28..12004.31 rows=10 width=24) (actual time=151.712..151.714 rows=10 loops=1)
--  ->  Sort  (cost=12004.28..12030.17 rows=10354 width=24) (actual time=151.710..151.712 rows=10 loops=1)
--        Sort Key: (count(ticket_no)) DESC
--        Sort Method: top-N heapsort  Memory: 26kB
--        ->  HashAggregate  (cost=11677.00..11780.54 rows=10354 width=24) (actual time=146.029..148.866 rows=22220 loops=1)
--              Group Key: passenger_name
--              Batches: 1  Memory Usage: 2577kB
--              ->  Seq Scan on tickets  (cost=0.00..9843.33 rows=366733 width=30) (actual time=0.007..26.855 rows=366733 loops=1)
--Planning Time: 0.113 ms
--Execution Time: 151.755 ms


--explain
--Limit  (cost=12003.17..12003.20 rows=10 width=24)
--  ->  Sort  (cost=12003.17..12028.97 rows=10319 width=24)
--        Sort Key: (count(ticket_no)) DESC
--        ->  HashAggregate  (cost=11677.00..11780.19 rows=10319 width=24)
--              Group Key: passenger_name
--              ->  Seq Scan on tickets  (cost=0.00..9843.33 rows=366733 width=30)


CREATE INDEX idx_tickets_passenger_name_ticket_no ON bookings.tickets(passenger_name, ticket_no);

--explain analyze
--Limit  (cost=16926.37..16926.39 rows=10 width=24) (actual time=105.866..105.868 rows=10 loops=1)
--  ->  Sort  (cost=16926.37..16952.25 rows=10354 width=24) (actual time=105.865..105.866 rows=10 loops=1)
--        Sort Key: (count(ticket_no)) DESC
--        Sort Method: top-N heapsort  Memory: 25kB
--        ->  GroupAggregate  (cost=0.42..16702.62 rows=10354 width=24) (actual time=0.030..102.533 rows=22220 loops=1)
--              Group Key: passenger_name
--              ->  Index Only Scan using idx_tickets_passenger_name_ticket_no on tickets  (cost=0.42..14765.42 rows=366733 width=30) (actual time=0.026..47.238 rows=366733 loops=1)
--                    Heap Fetches: 0
--Planning Time: 0.194 ms
--Execution Time: 105.886 ms

--explain
--Limit  (cost=16925.26..16925.29 rows=10 width=24)
--  ->  Sort  (cost=16925.26..16951.06 rows=10319 width=24)
--        Sort Key: (count(ticket_no)) DESC
--        ->  GroupAggregate  (cost=0.42..16702.27 rows=10319 width=24)
--              Group Key: passenger_name
--              ->  Index Only Scan using idx_tickets_passenger_name_ticket_no on tickets  (cost=0.42..14765.42 rows=366733 width=30)

SET enable_seqscan = off;
reset enable_seqscan

-- Вывод: использование индекса эффективно, но автоматически не применяется, так как в теоритической оценке стоимости затрат на запрос 
-- показатели с индексом выше cost=16925.26..16925.29, чем без него cost=12003.17..12003.20
-- Решения: 
-- 1. Использование SET enable_seqscan = off (не рекомендуется)
-- 2. Если задача часто используется, можно создать материализованное представление, обновляемое по расписанию.



--Задача 2 
--Анализ загрузки самолетов по дням недели
--Определить, в какие дни недели самолеты загружены больше всего.
--
--Логика расчета. Шаги:
--- Используем данные о занятых местах (boarding_passes) и общем количестве мест (seats).
--- Группируем данные по дням недели.
--- Рассчитаем среднюю загрузку самолетов для каждого дня.

select 
    to_char(f.scheduled_departure, 'day') as weekday, 			-- превращаем дату вылета в строку с названием дня недели
    round(avg(bp_count::decimal / seat_count), 3) as avg_load   -- расчитываем среднюю загрузку рейса
from bookings.flights f
join (															-- подзапрос: считает, сколько посадочных талонов (значит, пассажиров) было на каждом рейсе
    select 
    	flight_id, 
    	count(*) as bp_count
    from bookings.boarding_passes
    group by flight_id
) bp 
on f.flight_id = bp.flight_id
join (															-- подзапрос: считает, сколько всего мест в каждом самолёте
    select 
    	aircraft_code, 
    	count(*) as seat_count
    from bookings.seats
    group by aircraft_code
) s 
on f.aircraft_code = s.aircraft_code
group by weekday												-- группируем результаты по дню недели, чтобы потом по каждой группе посчитать среднюю загрузку
order by avg_load desc;											-- сортируем результат — от самого загруженного дня к наименее загруженному.







--Задача 3. Узнать что такое GroupAggregate и придумать пример имитации его

--GroupAggregate — это операция в плане выполнения запроса PostgreSQL, которая используется для выполнения агрегатных функций на группе строк,
--когда мы используем GROUP BY

--Например, если у нас есть список товаров, и мы хотим посчитать, сколько стоит каждый товар в сумме, то мы будем группировать товары 
--по их категории (например, "фрукты", "овощи"), а затем для каждой группы вычислять итоговую цену.


-- 1. Создаем индекс по полю группировки
CREATE INDEX idx_flights_aircraft_code ON bookings.flights (aircraft_code);

-- 2. Запускаем запрос, где данные уже отсортированы
EXPLAIN ANALYZE
SELECT aircraft_code, COUNT(*)
FROM bookings.flights
GROUP BY aircraft_code
ORDER BY aircraft_code;

--GroupAggregate  (cost=0.29..801.88 rows=8 width=12) (actual time=0.304..7.492 rows=8 loops=1)
--  Group Key: aircraft_code
--  ->  Index Only Scan using idx_flights_aircraft_code on flights  (cost=0.29..636.19 rows=33121 width=4) (actual time=0.034..3.046 rows=33121 loops=1)
--        Heap Fetches: 211
--Planning Time: 0.295 ms
--Execution Time: 7.512 ms

-- Чтение плана запроса:
--1. Index Only Scan
--PostgreSQL использует индекс idx_flights_aircraft_code, чтобы считать значения aircraft_code без чтения всей строки из таблицы (это ускоряет запрос).
--Index Only Scan читается напрямую из B-дерева индекса.
--Heap Fetches: 211 означает, что в 211 случаях всё же пришлось лезть в таблицу — вероятно, страницы индекса были неактуальны в MVCC.

--2. GroupAggregate
--Агрегация по aircraft_code, т.е. GROUP BY.
--Выбран метод GroupAggregate, потому что:
--Входные данные уже отсортированы (спасибо Index Only Scan по aircraft_code);
--Значит, можно группировать на лету, без использования хэшей.



--Почему PostgreSQL чаще использует HashAggregate, а не GroupAggregate, даже при GROUP BY?
--Потому что HashAggregate работает быстрее, если:
--Данные не отсортированы,
--Много разных групп,
--И мало памяти нужно на каждую группу.

--Что действительно влияет на выбор GroupAggregate:
--Данные уже отсортированы по GROUP BY-полю (например, по aircraft_code)
--Или есть используемый индекс по GROUP BY-полю
--Или отключаем HashAggregate принудительно: SET enable_hashagg = off;