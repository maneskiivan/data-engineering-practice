CREATE TABLE [IF NOT EXISTS] accounts (
  customer_id INT PRIMARY KEY UNIQUE NOT NULL,
  first_name VARCHAR (50) NOT NULL,
  last_name VARCHAR (50) NOT NULL,
  address_1 VARCHAR (120),
  address_2 VARCHAR (120),
  city VARCHAR (50),
  state VARCHAR (50),
  zip_code INT,
  join_date DATE NOT NULL
);
