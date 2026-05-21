-- run in spark sql shell

CREATE DATABASE IF NOT EXISTS demo.ecommerce;

USE demo.ecommerce;


-- create partitioned table

CREATE TABLE orders_partitioned (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    total_amount DECIMAL(10, 2),
    status STRING
)
USING iceberg
PARTITIONED BY (months(order_date))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

-- insert test data

INSERT INTO orders_partitioned
SELECT
    row_num AS order_id,
    1 + (row_num % 100) AS customer_id,
    date_add(DATE '2025-01-01', row_num % 180) AS order_date,
    ROUND(CAST((10 + (row_num % 500)) AS DECIMAL(10,2)) + (row_num % 100) / 100.0, 2) AS total_amount,
    CASE (row_num % 4)
        WHEN 0 THEN 'completed'
        WHEN 1 THEN 'completed'
        WHEN 2 THEN 'pending'
        ELSE 'cancelled'
    END AS status
FROM (SELECT explode(sequence(1, 1000)) AS row_num);



-- test data

SELECT
    DATE_TRUNC('month', order_date) AS month,
    COUNT(*) AS order_count
FROM orders_partitioned
GROUP BY 1
ORDER BY 1;



-- check partition info

select partition,record_count,file_count,total_data_file_size_in_bytes from demo.ecommerce.orders_partitioned.partitions;


SELECT
    partition,
    file_path,
    record_count,
    file_size_in_bytes
FROM demo.ecommerce.orders_partitioned.files;

------------------------------------------------------ check partition pruning----------------------------------------

EXPLAIN COST
SELECT * FROM orders_partitioned;

-- Statistics(sizeInBytes=46.9 KiB, rowCount=1.00E+3)
-- This shows Iceberg will scan ~47 KB across all 1,000 rows (all six partitions).

EXPLAIN COST
SELECT * FROM orders_partitioned
WHERE order_date >= DATE '2025-03-01'
  AND order_date < DATE '2025-04-01';

-- Statistics(sizeInBytes=8.7 KiB, rowCount=186)
