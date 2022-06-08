CREATE TABLE transactions (
  transaction_id INT PRIMARY KEY UNIQUE NOT NULL,
  transaction_date DATE NOT NULL,
  product_id INT REFERENCES products (product_id),
  product_code INT NOT NULL,
  product_description VARCHAR (120),
  quantity INT,
  account_id INT REFERENCES accounts(customer_id),
  UNIQUE (transaction_id, product_id, account_id)
);
