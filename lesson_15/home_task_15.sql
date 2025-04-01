CREATE TABLE books (
	book_id integer,
	book_name text,
	year_of_publication integer,
	author_id integer,
	quantity_in_stock integer,
	status text
);

CREATE TABLE authors (
	author_id integer,
	full_author_name text,
	birth_year integer,
	country_of_origin text,
	brief_biography text	
);

CREATE TABLE readers (
	reader_id integer,
	full_reader_name text,
	library_card_number integer,
	registration_date date,
	phone_number text,
	email text
);


INSERT INTO books (book_id, book_name, year_of_publication, author_id, quantity_in_stock, status) VALUES
	(1, 'Любителям читать', 1980, 5, 20, 'доступна'),
	(2, 'К себе нежно', 2020, 2, 1, 'доступна'),
	(3, 'Откройте двери', 2018, 1, 9, 'выдана'),
	(4, 'Убийца на улице Беверли', 2020, 3, 1, 'выдана'),
	(5, 'Тайны домашних животных', 2010, 4, 45, 'выдана');

INSERT INTO authors (author_id, full_author_name, birth_year, country_of_origin, brief_biography) VALUES
	(1, 'Тимофеева Мария Александровна', 1945, 'Беларусь', 'Провела детство в небольшом городке, увлекалась философией и научной фантастикой.'),
	(2, 'Кузнецов Дмитрий Романович', 1980, 'Россия', 'Родился в Москве, всегда интересовался историей и литературой. Пишет с юных лет'),
	(3, 'Михайлова Ольга Николаевна', 2000, 'Беларусь', 'Выросла в Минске, изучает культуру и традиции народов мира. Вдохновляется путешествиями.'),
	(4, 'Смирнова Елена Вячеславовна', 1967, 'Беларусь', 'Родилась в Бресте, много времени проводила за книгами, интересуется психоанализом и культурной антропологией.'),
	(5, 'Василенко Анна Ивановна', 1999, 'Россия', 'Жила в Санкт-Петербурге, окончила университет по специальности журналистика.');

INSERT INTO readers (reader_id, full_reader_name, library_card_number, registration_date, phone_number, email) VALUES
	(1, 'Петрова Екатерина Анатольевна', 123, '2024-10-01', '80299876543', 'kate.morgan24@gmail.com'),
	(2, 'Соколова Анастасия Николаевна', 890, '2025-02-23', '80331245423', 'sarah.johnson_work@mail.com'),
	(3, 'Николаев Владимир Владимирович', 20, '2023-08-06', '80296413498', 'tech.support2025@service.org'),
	(4, 'Козлова Татьяна Сергеевна', 65, '2024-09-09', '80294107392', 'kozlova@gmail.com'),
	(5, 'Шевченко Юрий Андреевич', 100, '2025-03-03', '80337395517', 'hello.world_123@mail.com');



SELECT * FROM books WHERE author_id=3;

SELECT * FROM books WHERE status='доступна';

SELECT * FROM readers WHERE registration_date < '2025-01-01';