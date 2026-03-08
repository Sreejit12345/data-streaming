
-- Demonstrate stateful processing using upsert sink. See how customer totals get changed

-- calculate total orders per customer, latest total order amount, latest order number of items,latest order_time


-- sample json

{
  "order_id": "ord-1001",
  "customer_id": "cust-5001",
  "order_time": "2026-03-08 10:21:33",
  "items": [
    {
      "sku": "SKU-IPHONE15",
      "product_name": "iPhone 15",
      "quantity": 1,
      "unit_price": 120000
    },
    {
      "sku": "SKU-AIRPODS",
      "product_name": "AirPods Pro",
      "quantity": 2,
      "unit_price": 24900
    }
  ],
  "shipping_address": {
    "city": "Bangalore",
    "state": "Karnataka",
    "country": "India",
    "postal_code": "560067"
  },
  "payment": {
    "method": "UPI",
    "provider": "PhonePe",
    "transaction_id": "txn-829182",
    "status": "SUCCESS"
  },
  "device": {
    "device_type": "mobile",
    "os": "android",
    "app_version": "5.3.1",
    "ip_address": "103.45.212.33"
  },
  "metadata": {
    "source_service": "order-service",
    "region": "ap-south-1",
    "trace_id": "trace-9234",
    "retry_count": 0
  }
}

-- create table based on json

CREATE TABLE orders_complex (
    order_id STRING,
    customer_id STRING,
    order_time TIMESTAMP(3),
    items ARRAY<ROW<
        sku STRING,
        product_name STRING,
        quantity INT,
        unit_price DOUBLE
    >>,
    shipping_address ROW<
        city STRING,
        state STRING,
        country STRING,
        postal_code STRING
    >,
    payment ROW<
        `method` STRING,
        provider STRING,
        transaction_id STRING,
        status STRING
    >,
    device ROW<
        device_type STRING,
        os STRING,
        app_version STRING,
        ip_address STRING
    >,
    metadata MAP<STRING,STRING>
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders_complex',
    'properties.bootstrap.servers' = 'broker:9092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

--create topic
 docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic orders_complex --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

 --produce (remeber to convert json to 1 line)  produce multiple messages per customer.
 docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --topic orders_complex --bootstrap-server localhost:9092


-- below query is correct, but since this is streaming the cte is again re-evaluated in latest_customer and hence gives 1 row and
-- not grouped result

 WITH cte AS ( 
    SELECT 
        order_id,
        customer_id,
        order_time,
        SUM(item_price) AS order_price,
        COUNT(*) AS num_items_in_order
    FROM (
        SELECT 
            order_id,
            customer_id,
            order_time,
            (m.quantity * m.unit_price) AS item_price
        FROM orders_complex
        CROSS JOIN UNNEST(items) AS m
    ) q
    GROUP BY order_id, customer_id, order_time
)
, cust_level AS (
    SELECT customer_id, COUNT(order_id) AS order_ct
    FROM cte
    GROUP BY customer_id
)
, latest_customer AS (
    SELECT customer_id, order_id,order_price, num_items_in_order
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY customer_id,order_id ORDER BY order_time DESC) AS rn
        FROM cte
    ) w
    WHERE rn = 1
    gri
)

SELECT lc.*, c.order_ct
FROM latest_customer lc
JOIN cust_level c
ON lc.customer_id = c.customer_id;


-- better way, works in streaming;

-- create table (upsert) mode

 CREATE TABLE customer_latest
(

    customer_id STRING,
    latest_order_id STRING,
    latest_order_time TIMESTAMP(3),
    latest_order_price DOUBLE,
    latest_num_items_in_order BIGINT,
    total_orders BIGINT,
     PRIMARY KEY (customer_id) NOT ENFORCED
)
WITH
(
'connector' ='upsert-kafka',
'topic'='customer_latest',
'properties.bootstrap.servers'='broker:9092',
'key.format'='json',
'value.format'='json'
)
;

-- create topic . Remeber this is always append only, flink sql treats as upsert.

docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic customer_latest --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1


-- write upsert to sink

insert into customer_latest
 WITH cte AS (
    SELECT 
        order_id,
        customer_id,
        order_time,
        SUM(item_price) AS order_price,
        COUNT(*) AS num_items_in_order
    FROM (
        SELECT 
            order_id,
            customer_id,
            order_time,
            (m.quantity * m.unit_price) AS item_price
        FROM orders_complex
        CROSS JOIN UNNEST(items) AS m
    ) q
    GROUP BY order_id, customer_id, order_time
)
, cust_level AS (
    SELECT customer_id, COUNT(order_id) as order_num ,max(order_time) as latest_order
    FROM cte
    GROUP BY customer_id
)
, latest_customer AS (
select c.customer_id,c.order_id as latest_order_id,c.order_time as latest_order_time,
c.order_price as latest_order_price,c.num_items_in_order as latest_num_items_in_order
,c1.order_num as total_orders from cust_level c1
join cte c
on(c1.customer_id=c.customer_id)
and(c.order_time=c1.latest_order)
)
select * from latest_customer;

-- read from table (notice the upserts)

SET 'sql-client.execution.result-mode' = 'table'; 
select * from customer_latest;

-- consume from customer_latest and confirm that its append only


docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --topic customer_latest --bootstrap-server localhost:9092 --from-beginning