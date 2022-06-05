CREATE TABLE [IF NOT EXISTS] transactions (
  transaction_id INT PRIMARY KEY UNIQUE NOT NULL,
  transaction_date DATE NOT NULL,
  CONSTRAINT product_id
    FOREIGN KEY (product_id)
      REFERENCES products(product_id)
      ON DELETE CASCADE,
  CONSTRAINT product_code
    FOREIGN KEY (product_code)
      REFERENCES products(product_code)
      ON DELETE CASCADE,
  CONSTRAINT product_description
    FOREIGN KEY (product_description)
      REFERENCES products(product_description)
      ON DELETE CASCADE,
  quantity INT NOT NULL,
  CONSTRAINT account_id
    FOREIGN KEY (customer_id)
      REFERENCES accounts(customer_id)
      ON DELETE CASCADE
);
