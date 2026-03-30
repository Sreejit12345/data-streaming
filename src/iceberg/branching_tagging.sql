CREATE DATABASE IF NOT EXISTS finance;
USE demo.finance;

DROP TABLE IF EXISTS transactions;

CREATE TABLE transactions (
    transaction_id BIGINT,
    account_id BIGINT,
    transaction_date DATE,
    amount DECIMAL(15, 2),
    description STRING,
    verified BOOLEAN
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

INSERT INTO transactions VALUES
    (1001, 100, CAST('2025-01-02' AS DATE), 5000.00, 'Initial deposit', true),
    (1002, 100, CAST('2025-01-05' AS DATE), -150.00, 'Office supplies', true),
    (1003, 101, CAST('2025-01-03' AS DATE), 12000.00, 'Initial deposit', true),
    (1004, 101, CAST('2025-01-10' AS DATE), -3200.00, 'Equipment purchase', true),
    (1005, 102, CAST('2025-01-04' AS DATE), 8500.00, 'Initial deposit', true);

SELECT * FROM transactions ORDER BY transaction_id;


-- create a tag 

ALTER TABLE transactions CREATE TAG `jan-2025-close`;   -- this creates a tag on the current snapshot


-- check refs

SELECT * FROM demo.finance.transactions.refs;

--Notice that main is also listed—it's the default branch that every Iceberg table has.

-- insert normal transactions into main

INSERT INTO transactions VALUES
    (1006, 100, CAST('2025-02-01' AS DATE), -89.99, 'Software subscription', true),
    (1007, 102, CAST('2025-02-03' AS DATE), -1200.00, 'Contractor payment', true),
    (1008, 101, CAST('2025-02-05' AS DATE), 4500.00, 'Client payment received', true);


-- query tagged snapshot
SELECT *
FROM transactions VERSION AS OF 'jan-2025-close'
ORDER BY transaction_id;

-- query main
SELECT *
FROM transactions
ORDER BY transaction_id;


----------------------------------------------------------- Branching ----------------------------------------------------------

ALTER TABLE transactions CREATE BRANCH staging;  -- this branch will start at current snapshot

-- check refs
SELECT name, type, snapshot_id
FROM demo.finance.transactions.refs;   -- note main and staging are at the same snapshot


-- WAP pattern

ALTER TABLE demo.finance.transactions
    SET TBLPROPERTIES ('write.wap.enabled' = 'true');

SET spark.wap.branch = staging;

-- remember these entries go to "staging" and not to "main"
INSERT INTO transactions VALUES
    (1009, 100, CAST('2025-02-10' AS DATE), -500.00, 'Consulting fee', false),
    (1010, 103, CAST('2025-02-10' AS DATE), 15000.00, 'New client deposit', false),
    (1011, 101, CAST('2025-02-11' AS DATE), -750.00, 'Travel expenses', false),
    (1012, 100, CAST('2025-02-12' AS DATE), 2000.00, 'Refund received', false);



-- Production (main branch) - need to query explicitly
SELECT COUNT(*) AS main_count
FROM transactions VERSION AS OF 'main';

-- Staging branch (current write target, also queryable by name)   w/o any name it queries staging branch
SELECT COUNT(*) AS staging_count
FROM transactions VERSION AS OF 'staging';



-- DQ checks

-- Check for any negative balances (business rule violation)
SELECT
    account_id,
    SUM(amount) AS balance
FROM transactions VERSION AS OF 'staging'
GROUP BY account_id
HAVING SUM(amount) < 0;

-- check for unverified transactions

SELECT *
FROM transactions VERSION AS OF 'staging'
WHERE verified = false;  -- all the new trancations are unverified

--Let's mark the transactions in staging as verified (remember, we're still writing to staging):

UPDATE transactions
SET verified = true
WHERE verified = false;


---- now we need to publish staged changes


CALL system.fast_forward('demo.finance.transactions', 'main', 'staging');   -- just a metadata/pointer change


ALTER TABLE demo.finance.transactions
    UNSET TBLPROPERTIES ('write.wap.enabled');   -- unset WAP mode, so all writes go to main


-- check all entries in main

SELECT * FROM transactions ORDER BY transaction_id;



-- check refs and see staging and main both point to same(latest) snapshot

SELECT name, type, snapshot_id
FROM demo.finance.transactions.refs;
