/*
1. Создание таблицы promocodes
Что нужно сделать:
Создать таблицу для хранения промокодов, которые могут применять пользователи к своим заказам.

Требования:
promo_id – уникальный ID промокода (первичный ключ, автоинкремент).
code – текст промокода (уникальный, не может повторяться).
discount_percent – процент скидки (от 1 до 100).
valid_from и valid_to – срок действия.
max_uses – максимальное количество применений (NULL = без ограничений).
used_count – сколько раз уже использован (по умолчанию 0).
is_active – активен ли промокод (1 или 0, по умолчанию 1).
created_by – кто создал промокод (внешний ключ на users.id).
*/


CREATE TABLE if not EXISTS promocodes(
	promo_id INTEGER PRIMARY KEY AUTOINCREMENT,
	code TEXT UNIQUE NOT NULL,
	discount_percent INTEGER CHECK (discount_percent BETWEEN 1 AND 100),
	valid_from TEXT NOT NULL,
	valid_to TEXT NOT NULL,
	max_uses INTEGER,
	used_count INTEGER DEFAULT 0,
	is_active INTEGER CHECK (is_active IN (1,0)) DEFAULT 1,
	created_by INTEGER,
	
	FOREIGN KEY (created_by) REFERENCES users(id)
);

/*
2. Заполнение таблицы тестовыми данными
- Добавить 10 тестовых промокодов с разными параметрами:
- Скидки от 5% до 50%.
- Разные даты действия (некоторые уже истекли).
- Ограниченное и неограниченное количество использований.
*/

INSERT INTO promocodes(code, discount_percent, valid_from, valid_to, max_uses, created_by) VALUES
	('SUMMER10', 10, '2025-06-01', '2025-08-31', 100, 1),
    ('WELCOME20', 20, '2025-01-01', '2025-12-31', NULL, 2),
    ('BLACKFRIDAY30', 30, '2025-11-24', '2025-11-27', 500, 3),
    ('NEWYEAR15', 15, '2025-12-20', '2026-01-10', 200, 4),
    ('FLASH25', 25, '2025-10-01', '2025-10-07', 50, 5),
    ('LOYALTY5', 5, '2025-01-01', '2025-01-01', NULL, 6),
    ('MEGA50', 50, '2025-09-01', '2025-09-30', 10, 7),
    ('AUTUMN20', 20, '2025-09-01', '2025-11-30', 300, 8),
    ('SPRING10', 10, '2025-03-01', '2025-05-31', 150, 9),
    ('VIP40', 40, '2025-07-01', '2025-07-31', 20, 10);

/*
3. Анализ по группам скидок
Сгруппировать промокоды по диапазонам скидок и вывести:
- Количество промокодов в группе.
- Минимальную и максимальную скидку.
- Сколько из них имеют ограничение по использованию (max_uses IS NOT NULL).
 */


SELECT
	CASE
		WHEN discount_percent < 10 THEN '<10'
		WHEN discount_percent BETWEEN 10 AND 20 THEN '10-20'
		WHEN discount_percent BETWEEN 20 AND 30 THEN '20-30'
		WHEN discount_percent BETWEEN 30 AND 40 THEN '30-40'
		WHEN discount_percent > 40 THEN '>40'
	END AS discount_percent_group,
	COUNT(*) AS cnt,
	MIN(discount_percent) AS minimal_percent,
	MAX(discount_percent) AS maximum_percent,
	COUNT(CASE WHEN max_uses IS NOT NULL THEN 1 END) AS count_with_max_uses
FROM promocodes
GROUP BY discount_percent_group;
	

/*
4. Анализ по времени действия
Что нужно сделать:
Разделить промокоды на:
- Активные (текущая дата между valid_from и valid_to).
- Истекшие (valid_to < текущая дата).
- Еще не начавшиеся (valid_from > текущая дата).
Для каждой группы вывести:
- Количество промокодов.
- Средний процент скидки.
- Сколько из них имеют лимит использований.
*/

SELECT
	CASE
		WHEN CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to THEN 'Активные'
		WHEN CURRENT_TIMESTAMP > valid_to THEN 'Истекшие'
		WHEN CURRENT_TIMESTAMP < valid_from THEN 'Еще не начавшиеся'
	END AS time_group,
	count(*) AS cnt,
	ROUND(AVG(discount_percent), 0) AS average_discount_percent,
	COUNT(CASE WHEN max_uses IS NOT NULL THEN 1 END) AS count_with_max_uses
FROM promocodes
GROUP BY time_group;


/*
5* Базовая статистика по промокодам
Посчитать:
- Общее количество промокодов.
- Средний процент скидки .
- Количество активных промокодов.
*/

SELECT 
	COUNT(*) AS cnt,
	ROUND(AVG(discount_percent), 0) AS average_discount_percent,
	COUNT(CASE WHEN CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to THEN 1 END) AS active_count
FROM promocodes