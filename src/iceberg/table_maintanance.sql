CREATE DATABASE IF NOT EXISTS ecommerce;

USE demo.ecommerce;

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    properties STRING
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version'='2',
    'write.format.default'='parquet',
    'write.update.mode'='copy-on-write',
    'write.delete.mode'='merge-on-read'
);


-- simulate prod traffic
INSERT INTO events VALUES
    (1, 101, 'page_view', CAST('2025-01-15 10:00:00' AS TIMESTAMP), '{"page": "/home"}'),
    (2, 102, 'page_view', CAST('2025-01-15 10:01:00' AS TIMESTAMP), '{"page": "/products"}');

-- Batch 2
INSERT INTO events VALUES
    (3, 101, 'click', CAST('2025-01-15 10:02:00' AS TIMESTAMP), '{"button": "add_to_cart"}'),
    (4, 103, 'page_view', CAST('2025-01-15 10:03:00' AS TIMESTAMP), '{"page": "/checkout"}');

-- Batch 3
INSERT INTO events VALUES
    (5, 102, 'purchase', CAST('2025-01-15 10:05:00' AS TIMESTAMP), '{"amount": 99.99}'),
    (6, 101, 'page_view', CAST('2025-01-15 10:06:00' AS TIMESTAMP), '{"page": "/confirmation"}');

-- Batch 4
INSERT INTO events VALUES
    (7, 104, 'page_view', CAST('2025-01-15 10:10:00' AS TIMESTAMP), '{"page": "/home"}'),
    (8, 104, 'click', CAST('2025-01-15 10:11:00' AS TIMESTAMP), '{"button": "signup"}');

-- Batch 5
INSERT INTO events VALUES
    (9, 104, 'signup', CAST('2025-01-15 10:12:00' AS TIMESTAMP), '{"method": "email"}'),
    (10, 105, 'page_view', CAST('2025-01-15 10:15:00' AS TIMESTAMP), '{"page": "/products"}');


-- inspect files metadata

SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-data-files'] AS files_added,
    summary['total-records'] AS total_records
FROM demo.ecommerce.events.snapshots
ORDER BY committed_at; -- since we had 5 batches we have 5 snapshots . 1 per insert


SELECT
    file_path,
    record_count,
    file_size_in_bytes
FROM demo.ecommerce.events.files;  -- we see 5 files very small size


-- now we simulate updates (remember copy on write creates a new file for change)

UPDATE events
SET event_type = 'page_view'
WHERE event_id = 3;

-- Update properties
UPDATE events
SET properties = '{"page": "/home", "referrer": "google"}'
WHERE event_id = 1;

-- Mark an event as processed
UPDATE events
SET properties = '{"amount": 99.99, "processed": true}'
WHERE event_id = 5;


-- inspect metadata

'''

We can see 3 new snapshots (one per UPDATE), bringing the total to 8 snapshots.
Each UPDATE operation replaced a data file with an updated version. 
The current snapshot still shows ~5 data files, 
but the old versions of those files are now orphaned (only referenced by previous snapshots).

'''
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['total-data-files'] AS data_files
FROM demo.ecommerce.events.snapshots
ORDER BY committed_at;   

-- now we do deletes---- (merge on read for deletes, marks rows as deleted and writes delete files and applies them during query)

-- Delete some events
DELETE FROM events
WHERE event_id IN (7, 8);

-- Delete another event
DELETE FROM events
WHERE event_id = 10;

-- check metadata

SELECT
    content,
    file_path,
    record_count,
    file_size_in_bytes
FROM demo.ecommerce.events.files
ORDER BY content, file_path;

'''

Look at the content column:

0 = Data file (contains actual rows)
1 = Positional delete file (marks rows as deleted)
You should see new files with content = 1. These are delete files that record which positions (row numbers) in data files
 have been deleted.

Key insight: With merge-on-read mode, Iceberg doesnt immediately rewrite data files when you delete rows. 
Instead, it creates small delete files that query engines must read and apply.
 This is efficient for small deletes but accumulates overhead over time.

'''


-------- file compaction -------------------------------------------

CALL system.rewrite_data_files(
    table => 'demo.ecommerce.events',
    options => map(
        'rewrite-all', 'true'
    )
);   -- copmaction also applies deletes and writes files without deleted rows, but the delete files still remain post this


-------- expire snapshots --------------------------------------------------

SELECT MAX(committed_at) FROM demo.ecommerce.events.snapshots;

CALL system.expire_snapshots(
    table => 'demo.ecommerce.events',
    older_than => TIMESTAMP '2026-03-31 13:11:32.047',  -- replace with the max timestamp from above
    retain_last => 1  -- Always keep at least this many snapshots.
);   -- post this only the latest snapshot remains, old ones are gone


---- remove orphaned files --------------------------------------

--Snapshot expiration removes the metadata for old snapshots, but the actual data files they referenced are still on disk. 
-- These are now orphans—files that no snapshot references.

CALL system.remove_orphan_files(
    table => 'demo.ecommerce.events',
    older_than => TIMESTAMP '2025-03-31 13:11:32.047'  -- must be at least 24 hours ago
);

