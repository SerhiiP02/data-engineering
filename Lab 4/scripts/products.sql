CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_code VARCHAR(10),
    product_description VARCHAR(255)
);

-- Index
CREATE INDEX idx_product_id ON products(product_id);