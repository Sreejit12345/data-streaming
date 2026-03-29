-- run in spark-sql shell

show catalogs;

CREATE DATABASE IF NOT EXISTS demo.ecommerce;

show databases;

USE demo.ecommerce;

CREATE TABLE products (
    product_id INT,
    name STRING,
    price DECIMAL(10, 2)
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

describe extended demo.ecommerce.products;

INSERT INTO products VALUES
    (1, 'Wireless Mouse', 29.99),
    (2, 'Mechanical Keyboard', 89.99),
    (3, 'USB-C Hub', 49.99),
    (4, '27-inch Monitor', 349.99),
    (5, 'Webcam HD', 79.99);

select * from demo.ecommerce.products;

select snapshot_id,committed_at,operation
from demo.ecommerce.products.snapshots order by committed_at;



--------- add a new column ------------------------------------------------------------------

ALTER TABLE products
ADD COLUMN category STRING;


describe extended demo.ecommerce.products;

SELECT * FROM products ORDER BY product_id;  -- at this point the category will be null for old records


UPDATE products SET category = 'Peripherals' WHERE product_id IN (1, 2);
UPDATE products SET category = 'Accessories' WHERE product_id = 3;
UPDATE products SET category = 'Displays' WHERE product_id = 4;
UPDATE products SET category = 'Peripherals' WHERE product_id = 5;


-- check that new snapshots are created

select snapshot_id,committed_at,operation
from demo.ecommerce.products.snapshots order by committed_at;

-- insert as per new schema

INSERT INTO products VALUES
    (6, 'Laptop Stand', 59.99, 'Accessories');

SELECT * FROM products ORDER BY product_id;  -- all present

-- check snapshot

select snapshot_id,committed_at,operation
from demo.ecommerce.products.snapshots order by committed_at;


--------- rename an exisitng column ------------------------------------------------------------------

ALTER TABLE products
    RENAME COLUMN name TO product_name;


SELECT product_id, product_name, price FROM products ORDER BY product_id;  -- using new name

'''
The old Parquet files still have a column called name internally, 
but Icebergs metadata maps it to product_name. No file rewrites needed.
'''

------------ widen a columnn -------------------------------------

ALTER TABLE products ALTER COLUMN product_id TYPE BIGINT;  -- chaNge int to bigint


describe extended demo.ecommerce.products;

----- drop a columnn-------------------------------------------

ALTER TABLE products
DROP COLUMN category;

DESCRIBE products;

SELECT * FROM products ORDER BY product_id;


------ see schema history -----------------------

SELECT
    snapshot_id,
    committed_at,
    operation
FROM demo.ecommerce.products.snapshots
ORDER BY committed_at;


-- we cany copy the snapshot id and time travel - and see the version as of that snapshot


SELECT * FROM products VERSION AS OF 5576626934589082076;