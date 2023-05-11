CREATE TABLE customers(
    user_id INT,
    name VARCHAR,
    email VARCHAR
) WITH (
    'connector' = 'filesystem',
    'path' = 'users.csv',
    'format' = 'csv'
);

CREATE TABLE products(
    product_id INT,
    product VARCHAR,
    category VARCHAR,
    price FLOAT
) WITH (
    'connector' = 'filesystem',
    'path' = 'products.csv',
    'format' = 'csv'
);


SELECT * from customers;
SELECT * from products;


CREATE TABLE transactions (
    id STRING,
    user_id INT,
    amount BIGINT,
    product_id INT,
    `time` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'transactions',
  'value.format' = 'json',
  'properties.group.id' = 'myGroup',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092'
);

SELECT * from transactions;

SELECT * from transactions WHERE user_id = 1001;

SELECT
   user_id,
   COUNT(*) as transactions_count
FROM transactions
GROUP BY user_id;

SELECT t.id, t.user_id, c.name, t.amount, t.product_id
FROM transactions t
LEFT JOIN customers c ON t.user_id= c.user_id;

SELECT t.id,
    t.user_id,
    c.name,
    t.product_id,
    p.product,
    p.category,
    t.amount,
    p.price,
    t.amount*p.price as total
FROM transactions AS t
LEFT JOIN customers AS c ON t.user_id = c.user_id
LEFT JOIN products AS p ON t.product_id = p.product_id;
