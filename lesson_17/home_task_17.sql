/*
 
Задача 1: Анализ распределения мест в самолетах
- Необходимо проанализировать распределение мест в самолетах по классам обслуживания. Рассчитать:
- Общее количество мест в каждом самолете
- Количество мест каждого класса
- Процентное соотношение классов
- Массив всех мест для каждого самолета

*/
select * from bookings.seats;

--- Общее количество мест в каждом самолете
select
    aircraft_code,  -- Код самолета
    count(*) as total_seats  -- Общее количество мест
from 
    bookings.seats
group by 
    aircraft_code; 


-- Количество мест каждого класса
select
    fare_conditions,  -- Класс обслуживания
    count(*) as count_seats_class  -- Количество мест для этого класса
from 
    bookings.seats
group by 
    fare_conditions;  


-- Процентное соотношение классов в каждом самолете
select
    aircraft_code,  -- Код самолета
    (count(*) filter (where fare_conditions = 'Business') * 100.0 / count(*)) as percent_business_seats,  -- Процент мест Business
    (count(*) filter (where fare_conditions = 'Comfort') * 100.0 / count(*)) as percent_comfort_seats,  -- Процент мест Comfort
    (count(*) filter (where fare_conditions = 'Economy') * 100.0 / count(*)) as percent_economy_seats  -- Процент мест Economy
from 
    bookings.seats
group by 
    aircraft_code;  -- 
    

-- Массив всех мест для каждого самолета
select
    aircraft_code,  -- Код самолета
    array_agg(seat_no order by seat_no) as seats_array  -- Массив мест, отсортированных по seat_no
from 
    bookings.seats
group by 
    aircraft_code;  -- Группировка по коду самолета

    
 /*
  
Задача 2: Анализ стоимости билетов по рейсам
Для каждого рейса рассчитать:
- Минимальную, максимальную и среднюю стоимость билета
- Разницу между самым дорогим и самым дешевым билетом
- Медианную стоимость билета
- Массив всех цен на билеты

*/
    
select * from bookings.ticket_flights;


select
    flight_id,  -- Идентификатор рейса
    min(amount) as min_amount,  -- Минимальная стоимость билета
    max(amount) as max_amount,  -- Максимальная стоимость билета
    round(avg(amount), 2) as avg_amount,  -- Средняя стоимость билета, округленная до 2 знаков
    (max(amount) - min(amount)) as difference,  -- Разница между самой высокой и самой низкой стоимостью билетов
    percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount,  -- Медианная стоимость билета (0.5 - это медиана)
    array_agg(amount order by amount) as amount_array  -- Массив всех цен на билеты, отсортированных по возрастанию
from
    bookings.ticket_flights  -- Таблица рейсов с ценами билетов
group by
    flight_id;  -- Группируем по идентификатору рейса

 /*
  
Задача 3: Статистика по бронированиям по месяцам
- Проанализировать бронирования по месяцам:
- Количество бронирований
- Общую сумму бронирований
- Средний чек
- Массив всех сумм бронирований для анализа распределения

*/
 
    
select * from bookings.bookings;

select
    date_trunc('month', book_date) as month,  -- Приводим дату к месяцу
    count(*) as count_bookings,  -- Количество бронирований за месяц
    sum(total_amount) as total_sum,  -- Общая сумма бронирований за месяц
    sum(total_amount) / count(*) as avg_amount,  -- Средний чек за месяц
    array_agg(total_amount order by total_amount) as amount_array  -- Массив всех сумм бронирований, отсортированных по возрастанию
from 
    bookings.bookings  -- Таблица с данными по бронированиям
group by
    month;  -- Группировка по месяцу
    
    
 /*
  
 Задача 4: Анализ пассажиропотока по аэропортам
- Рассчитать для каждого аэропорта:
- Общее количество вылетов
- Количество уникальных аэропортов назначения
- Массив всех аэропортов назначения
- Процент международных рейсов (если известны коды стран)

*/

select * from bookings.flights;

select
    departure_airport,  -- Аэропорт отправления
    count(*) as count_flights,  -- Общее количество вылетов
    count(distinct arrival_airport) as unique_arrival_airports,  -- Количество уникальных аэропортов назначения
    array_agg(arrival_airport order by arrival_airport) as arrival_airport_array  -- Массив всех аэропортов назначения
from 
    bookings.flights
group by
    departure_airport  -- Группировка по аэропорту отправления
order by 
    departure_airport;  -- Сортировка по аэропорту отправления



/*
ДЗ:
Самостоятельно посмотреть: PERCENTILE_CONT и EPOCH, HAVING и еще найти 2-3 полезные функции
 */ 

    
-- 1. PERCENTILE_CONT - Функция для вычисления медианы
    
SELECT
    flight_id,  -- Идентификатор рейса
    percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount  -- Вычисляем медиану (перцентиль 50%) для стоимости билета
FROM
    bookings.ticket_flights  -- Из таблицы с информацией о билетах на рейсы
GROUP BY
    flight_id;  -- Группировка по рейсу для вычисления медианы каждого рейса


-- 2. EPOCH - Преобразование даты в количество секунд с 1970 года

SELECT
    booking_id,  -- Идентификатор бронирования
    EXTRACT(EPOCH FROM booking_date) AS booking_timestamp  -- Преобразуем дату бронирования в количество секунд с 1970 года
FROM
    bookings.bookings;  -- Из таблицы бронирований
    
    
-- 3. HAVING - Фильтрация данных после группировки

SELECT
    departure_airport,  -- Аэропорт вылета
    COUNT(*) AS flight_count  -- Количество рейсов из аэропорта
FROM
    bookings.flights  -- Из таблицы рейсов
GROUP BY
    departure_airport  -- Группируем по аэропорту вылета
HAVING
    COUNT(*) > 10;  -- Оставляем только аэропорты с более чем 10 рейсами
    
    
-- 4. ROW_NUMBER - Присвоение уникального номера строкам
    
SELECT
    flight_id,  -- Идентификатор рейса
    departure_time,  -- Время вылета
    ROW_NUMBER() OVER (ORDER BY departure_time) AS flight_number  -- Присваиваем уникальный номер рейсу в зависимости от времени вылета
FROM
    bookings.flights;  -- Из таблицы рейсов
    
    
-- 5. RANK - Ранжирование строк
    
SELECT
    participant_id,  -- Идентификатор участника
    score,  -- Баллы участника
    RANK() OVER (ORDER BY score DESC) AS rank  -- Присваиваем ранг участнику в зависимости от баллов, больше баллов — выше ранг
FROM
    competition.results;  -- Из таблицы результатов соревнования

-- 6. STRING_AGG - Объединение строк в одну

SELECT
    departure_airport,  -- Аэропорт вылета
    STRING_AGG(arrival_airport, ', ') AS arrival_airports  -- Все аэропорты назначения для рейса, объединенные в одну строку
FROM
    bookings.flights  -- Из таблицы рейсов
GROUP BY
    departure_airport;  -- Группировка по аэропорту вылета

-- 7. LEAD / LAG - Доступ к данным в предыдущей или следующей строке
    
SELECT
    flight_id,  -- Идентификатор рейса
    scheduled_departure,  -- Время вылета
    LEAD(scheduled_departure) OVER (PARTITION BY flight_id ORDER BY scheduled_departure) AS next_departure_time  -- Время следующего вылета
FROM
    bookings.flights;  -- Из таблицы рейсов

SELECT
    flight_id,  -- Идентификатор рейса
    scheduled_departure,  -- Время вылета
    LAG(scheduled_departure) OVER (PARTITION BY flight_id ORDER BY scheduled_departure) AS previous_departure_time  -- Время предыдущего вылета
FROM
    bookings.flights;  -- Из таблицы рейсов