CREATE TABLE [IF NOT EXISTS] products (
  product_id INT PRIMARY KEY UNIQUE NOT NULL,
  product_code INT UNIQUE NOT NULL,
  product_description VARCHAR (120) NOT NULL
);
