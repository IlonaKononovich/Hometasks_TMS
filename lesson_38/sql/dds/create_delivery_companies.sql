CREATE TABLE IF NOT EXISTS dds.delivery_companies (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT UNIQUE
);