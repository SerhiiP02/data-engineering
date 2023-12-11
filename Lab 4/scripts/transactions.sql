CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    transaction_date DATE,
    product_id INT,
    product_code VARCHAR(10),
    product_description VARCHAR(255),
    quantity INT,
    account_id INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
);

-- Index
CREATE INDEX idx_transaction_id ON transactions(transaction_id);