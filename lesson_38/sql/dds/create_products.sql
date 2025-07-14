
CREATE TABLE IF NOT EXISTS dds.products (
    product_id SERIAL PRIMARY KEY,
    product_name TEXT UNIQUE
);