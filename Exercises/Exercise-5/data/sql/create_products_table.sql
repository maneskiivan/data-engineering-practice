CREATE TABLE products (
  product_id INT PRIMARY KEY NOT NULL,
  product_code INT NOT NULL,
  product_description VARCHAR (120) NOT NULL,
  UNIQUE (product_id)
);

