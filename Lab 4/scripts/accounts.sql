CREATE TABLE accounts (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    address_1 VARCHAR(255),
    address_2 VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(10),
    join_date DATE
);

-- Index
CREATE INDEX idx_customer_id ON accounts(customer_id);