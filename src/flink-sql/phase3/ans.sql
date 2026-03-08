-- we look at kafka integration (stateless processing)

-- create a source (append) to ingest from orders topic
 
 CREATE TABLE orders_ip
(
    order_id STRING,
    order_amount DOUBLE,
    product_id STRING,
    customer_id STRING,
     order_timestamp TIMESTAMP(3)
)
WITH
( 
'connector' ='kafka',
'topic'='orders',
'properties.bootstrap.servers'='broker:9092',
'format'='json',
'scan.startup.mode'='latest-offset'
)
;

-- create a kafka topic for input

docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

-- start a producer for topic

docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 

--send sample events one by one

{"order_id":"ORD-1001","order_amount":259.99,"product_id":"PROD-501","customer_id":"CUST-301","order_timestamp":"2026-03-08 14:23:45.123"}
{"order_id":"ORD-1002","order_amount":499.50,"product_id":"PROD-502","customer_id":"CUST-302","order_timestamp":"2026-03-08 14:24:12.456"}
{"order_id":"ORD-1003","order_amount":129.75,"product_id":"PROD-503","customer_id":"CUST-303","order_timestamp":"2026-03-08 14:25:03.789"}   -- should not get written as amt is less





-- create a kafka topic for output
docker exec -it broker /opt/kafka/bin/kafka-topics.sh --create --topic orders_high_value --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

-- create a sink (append) to write from orders topic

 CREATE TABLE orders_op
(
    order_id STRING,
    order_amount DOUBLE,
    product_id STRING,
    customer_id STRING,
    order_timestamp TIMESTAMP(3)
)
WITH
(
'connector' ='kafka',
'topic'='orders_high_value',
'properties.bootstrap.servers'='broker:9092',
'format'='json',
'scan.startup.mode'='latest-offset'
)
;


SET 'execution.runtime-mode' = 'streaming';

-- start flink to ingest high value orders
insert into orders_op
select * from orders_ip where order_amount>=150;


-- query from sql
--start consumer from orders_high_value topic to see data

select * from orders_op ;  
docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh --topic orders_high_value --bootstrap-server localhost:9092 --from-beginning
