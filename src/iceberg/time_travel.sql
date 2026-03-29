
CREATE DATABASE IF NOT EXISTS demo.ecommerce;

USE demo.ecommerce;


CREATE TABLE customers (
    customer_id BIGINT,
    name STRING,
    email STRING,
    balance DECIMAL(10, 2),
    status STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

INSERT INTO customers VALUES
    (1, 'Alice Chen', 'alice@example.com', 1500.00, 'active'),
    (2, 'Bob Smith', 'bob@example.com', 2300.50, 'active'),
    (3, 'Carol Jones', 'carol@example.com', 890.25, 'active'),
    (4, 'David Lee', 'david@example.com', 4200.00, 'active'),
    (5, 'Eva Martinez', 'eva@example.com', 150.75, 'inactive');

SELECT * FROM customers ORDER BY customer_id;

-- check snapshot history

-- for now parent_id is null- since its the 1st snapshot
SELECT
    snapshot_id,
    parent_id,
    committed_at,
    operation
FROM demo.ecommerce.customers.snapshots
ORDER BY committed_at;

-- make some history---

UPDATE customers
SET balance = 1800.50
WHERE customer_id = 2;

UPDATE customers
SET balance = 2890.25
WHERE customer_id = 3;

UPDATE customers
SET balance = 0.00;


-- check snapshot version --

SELECT
    snapshot_id,
    parent_id,
    committed_at,
    operation
FROM demo.ecommerce.customers.snapshots
ORDER BY committed_at;

'''
You should see four snapshots:

The initial append (our INSERT)
An overwrite (Bobs update)
Another overwrite (Carols update)
The final overwrite (the accidental mass update)

Note the relation b/w snapshot_id and parent_id
'''


--- query table at a specific snapshot

SELECT *
FROM customers VERSION AS OF 7607195476920146040
ORDER BY customer_id;

--- query table at a specific timestamp (based on committed_at  values)

SELECT *
FROM customers TIMESTAMP AS OF '2026-03-29 12:57:29.870'
ORDER BY customer_id;

-- roll back table

CALL system.rollback_to_snapshot('demo.ecommerce.customers', 7607195476920146040);

-- check snapshot info

SELECT
    snapshot_id,
    committed_at,
    operation
FROM demo.ecommerce.customers.snapshots
ORDER BY committed_at;

'''

 Rollback updates the tables current snapshot pointer to point back to the previous snapshot
 it doesnt delete the bad snapshot or create a new one.

 '''

 SELECT * FROM demo.ecommerce.customers.refs;  -- check the current pointer , and which snapshot it points to
 