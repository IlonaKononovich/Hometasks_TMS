--- Создайте представление (VIEW) в схеме analytics с продажами по категориям
--- В запросе посчитать сумму продаж по категориям
--- Дайте доступ менеджерам только к этому представлению, а не ко всем таблицам
--- Создайте роль senior_analysts с правами аналитиков + возможностью создавать временные таблицы
--
--- Данными не обязательно заполнять таблицы, главное написать запрос

-- создание представления
create view analytics.sales_amount as
select 
    sum(ot.quantity * ot.price) as total_sales,
	p.category
from production.order_items ot
inner join production.products p
	on p.product_id = ot.product_id
group by p.category;


-- отзываем предоставленные права у менеджера
revoke ALL ON ALL TABLES IN SCHEMA production, analytics, sandbox FROM manager;

-- даем право на чтение представления
GRANT SELECT ON analytics.sales_amount TO manager;

-- создание роли senior_analysts с наследованием прав роли analytic
CREATE ROLE senior_analysts WITH INHERIT ROLE analytic;

-- присваиваем роли senior_analysts возможность создавать любые объекты (в том числе и временные таблицы)
GRANT CREATE ON SCHEMA production, analytics, sandbox TO senior_analysts;
