CREATE TABLE IF NOT EXISTS dds.deliveries (
    delivery_id TEXT PRIMARY KEY,
    order_id INTEGER REFERENCES dds.orders(order_id),
    product_id INTEGER REFERENCES dds.products(product_id),
    company_id INTEGER REFERENCES dds.delivery_companies(company_id),
    cost NUMERIC(10,2),
    courier_id INTEGER REFERENCES dds.couriers(courier_id),
    start_time DATE,
    end_time DATE,
    city TEXT,
    warehouse_id INTEGER REFERENCES dds.warehouses(warehouse_id)
);