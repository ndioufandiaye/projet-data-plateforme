CREATE TABLE IF NOT EXISTS sales (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(100),
    region VARCHAR(50),
    country VARCHAR(100),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    revenue DECIMAL(10, 2),
    profit DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);