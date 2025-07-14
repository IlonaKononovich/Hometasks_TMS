CREATE TABLE IF NOT EXISTS dds.orders (
    order_id INTEGER PRIMARY KEY,
    quantity INTEGER,
    price_per_unit NUMERIC(10,2),
    total_price NUMERIC(10,2),
    card_number TEXT,
    user_id TEXT REFERENCES dds.users(user_id),
    product_id INTEGER REFERENCES dds.products(product_id)
);