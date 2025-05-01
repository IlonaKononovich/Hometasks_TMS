-- Задача 1:
-- Необходимо оптимизировать выборку данных по номеру места (bookings.boarding_passes.seat_no) 
-- с помощью индекса и сравнить результаты до добавления индекса и после (время выполнения и объем таблицы в %)

explain analyze
select 
	seat_no
from bookings.boarding_passes
WHERE seat_no = '10A';

-- 1. До создания индекса 
--Gather  (cost=1000.00..8597.00 rows=2898 width=3) (actual time=0.269..27.940 rows=2655 loops=1)
--  Workers Planned: 2
--  Workers Launched: 2
--  ->  Parallel Seq Scan on boarding_passes  (cost=0.00..7307.20 rows=1208 width=3) (actual time=0.039..23.070 rows=885 loops=3)
--        Filter: ((seat_no)::text = '10A'::text)
--        Rows Removed by Filter: 192344
--Planning Time: 0.064 ms
--Execution Time: 28.200 ms

create index idx_boarding_passes_seat_no on bookings.boarding_passes (seat_no)


-- 2. После создания индекса
--Index Only Scan using idx_boarding_passes_seat_no on boarding_passes  (cost=0.42..115.14 rows=2898 width=3) (actual time=0.030..0.243 rows=2655 loops=1)
--  Index Cond: (seat_no = '10A'::text)
--  Heap Fetches: 0
--Planning Time: 0.196 ms
--Execution Time: 0.345 ms


SELECT pg_size_pretty(pg_relation_size('bookings.boarding_passes')); -- 34 MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_boarding_passes_seat_no')); -- 4032 kB


Ускорение = ((28.200 − 0.345) / 28.200 ) * 100% = 98.78 (%)
Ускорение в разах = 28.200 / 0.345 = 81.74 (раз)
​Доля индекса от таблицы = (4 / 34) * 100 = 11.76%


-- Вывод: После создания индекса idx_boarding_passes_seat_no, план запроса сменился с Parallel Seq Scan на Index Only Scan,
-- что ускорило выполнение с 28.2 ms до 0.345 ms — более чем в 81 раз быстрее (на ~98.8%).
-- Созданный индекс занимает 4 MB, что составляет около 11.8% от размера таблицы (34 MB). 
-- Это адекватная плата за значительное ускорение выборки по полю seat_no.







--Задача 2:
--1. Проанализировать производительность запроса без индексов.
--2. Добавить индексы для ускорения JOIN и фильтрации.
--3. Снова проанализировать производительность запроса и сравнить результаты.

explain analyze
SELECT bp.boarding_no, t.passenger_id
FROM bookings.boarding_passes bp
JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
JOIN bookings.seats s ON bp.seat_no = s.seat_no
JOIN bookings.bookings b ON t.book_ref = b.book_ref
WHERE 
  t.passenger_id in ('0856 579180', '4723 695013')
  and boarding_no < 100
;


--Gather  (cost=1005.15..9120.67 rows=15 width=16) (actual time=27.811..30.073 rows=65 loops=1)
--  Workers Planned: 2
--  Workers Launched: 2
--  ->  Nested Loop  (cost=5.15..8119.17 rows=6 width=16) (actual time=14.575..24.599 rows=22 loops=3)
--        ->  Nested Loop  (cost=4.87..8108.79 rows=1 width=19) (actual time=14.549..24.340 rows=3 loops=3)
--              ->  Nested Loop  (cost=4.45..8102.35 rows=1 width=26) (actual time=14.521..24.293 rows=3 loops=3)
--                    ->  Parallel Seq Scan on tickets t  (cost=0.00..8086.07 rows=1 width=33) (actual time=14.464..24.230 rows=1 loops=3)
--                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
--                          Rows Removed by Filter: 122244
--                    ->  Bitmap Heap Scan on boarding_passes bp  (cost=4.45..16.26 rows=3 width=21) (actual time=0.074..0.082 rows=5 loops=2)
--                          Recheck Cond: (ticket_no = t.ticket_no)
--                          Filter: (boarding_no < 100)
--                          ->  Bitmap Index Scan on boarding_passes_pkey  (cost=0.00..4.45 rows=3 width=0) (actual time=0.062..0.062 rows=5 loops=2)
--                                Index Cond: (ticket_no = t.ticket_no)
--              ->  Index Only Scan using bookings_pkey on bookings b  (cost=0.42..6.44 rows=1 width=7) (actual time=0.013..0.013 rows=1 loops=10)
--                    Index Cond: (book_ref = t.book_ref)
--                    Heap Fetches: 10
--        ->  Index Only Scan using seats_pkey on seats s  (cost=0.28..10.35 rows=3 width=3) (actual time=0.021..0.076 rows=6 loops=10)
--              Index Cond: (seat_no = (bp.seat_no)::text)
--              Heap Fetches: 0
--Planning Time: 0.649 ms
--Execution Time: 30.105 ms


CREATE INDEX idx_tickets_passenger_id ON bookings.tickets (passenger_id);  				
CREATE INDEX idx_boarding_passes_boarding_no ON bookings.boarding_passes (boarding_no); 


CREATE INDEX idx_boarding_passes_ticket_no ON bookings.boarding_passes (ticket_no);	    
CREATE INDEX idx_tickets_book_ref ON bookings.tickets (book_ref); 						
CREATE INDEX idx_seats_seat_no ON bookings.seats (seat_no); 						    
CREATE INDEX idx_bookings_book_ref ON bookings.bookings (book_ref); 					



--Nested Loop  (cost=5.57..67.41 rows=15 width=16) (actual time=0.107..0.223 rows=65 loops=1)
--  ->  Nested Loop  (cost=5.29..66.33 rows=3 width=19) (actual time=0.102..0.176 rows=10 loops=1)
--        ->  Nested Loop  (cost=0.84..33.76 rows=2 width=26) (actual time=0.073..0.112 rows=2 loops=1)
--              ->  Index Scan using idx_tickets_passenger_id on tickets t  (cost=0.42..16.88 rows=2 width=33) (actual time=0.048..0.073 rows=2 loops=1)
--                    Index Cond: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
--              ->  Index Only Scan using idx_bookings_book_ref on bookings b  (cost=0.42..8.44 rows=1 width=7) (actual time=0.017..0.017 rows=1 loops=2)
--                    Index Cond: (book_ref = t.book_ref)
--                    Heap Fetches: 2
--        ->  Bitmap Heap Scan on boarding_passes bp  (cost=4.45..16.26 rows=3 width=21) (actual time=0.024..0.029 rows=5 loops=2)
--              Recheck Cond: (ticket_no = t.ticket_no)
--              Filter: (boarding_no < 100)
--              Heap Blocks: exact=10
--              ->  Bitmap Index Scan on idx_boarding_passes_ticket_no  (cost=0.00..4.45 rows=3 width=0) (actual time=0.020..0.020 rows=5 loops=2)
--                    Index Cond: (ticket_no = t.ticket_no)
--  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.003..0.004 rows=6 loops=10)
--        Index Cond: (seat_no = (bp.seat_no)::text)
--        Heap Fetches: 0
--Planning Time: 1.420 ms
--Execution Time: 0.257 ms

SELECT pg_size_pretty(pg_relation_size('bookings.idx_tickets_passenger_id'));			-- 11   MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_boarding_passes_boarding_no'));	-- 4    MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_boarding_passes_ticket_no'));		-- 11   MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_tickets_book_ref'));				-- 7,5  MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_seats_seat_no'));					-- 0.04 MB
SELECT pg_size_pretty(pg_relation_size('bookings.idx_bookings_book_ref'))				-- 5,8  MB

SELECT pg_size_pretty(pg_relation_size('bookings.tickets')); 		 -- 48    MB
SELECT pg_size_pretty(pg_relation_size('bookings.boarding_passes')); -- 34    MB
SELECT pg_size_pretty(pg_relation_size('bookings.seats'));			 -- 0.064 MB
SELECT pg_size_pretty(pg_relation_size('bookings.bookings')); 		 -- 13    MB



Ускорение = ((30.105 − 0.257) / 30.105 ) * 100% = 99.14 (%)
Ускорение в разах = 30.105 / 0.257 = 117.14 (раз)
​Доля индексов от таблиц = (39.34 / 95.06) * 100% = 41.38%

-- Вывод: После создания индексов план запроса изменился,
-- что ускорило выполнение с 30.105 ms до 0.257 ms — более чем в 117 раз быстрее (на ~ 99%).
-- Созданные индексы занимают 39.34 MB, что составляет  41.38% от размера таблицы (95.06 MB).
-- Это хороший результат. Мы отдали 40% памяти, чтобы ускорить систему в 117 раз — отдача очень хорошая.