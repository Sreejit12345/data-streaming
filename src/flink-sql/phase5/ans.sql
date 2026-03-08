--Event Time watermark

-- handle order events around 30 seconds late

--The analytics team needs a real-time dashboard showing total revenue per minute based on the actual order time.


-- create table

CREATE TABLE orders (
    order_id STRING,
    order_amount DOUBLE,
    product_id STRING,
    customer_id STRING,
    order_timestamp TIMESTAMP(3),

    WATERMARK FOR order_timestamp AS order_timestamp - INTERVAL '30' SECOND
)
WITH (
 'connector' = 'faker',
 'rows-per-second' = '1',

 'fields.order_id.expression' = '#{regexify ''ord-[0-9]{5}''}',
 'fields.order_amount.expression' = '#{number.randomDouble ''2'',''100'',''5000''}',
 'fields.product_id.expression' = '#{regexify ''prod-[0-9]{4}''}',
 'fields.customer_id.expression' = '#{regexify ''cust-[0-9]{3}''}',

 'fields.order_timestamp.expression' ='#{date.past ''1'',''SECONDS''}'
);


-- query

select 
window_start,window_end,sum(order_amount) as revenue,window_time
FROM TABLE(
TUMBLE(DATA=> TABLE orders,
TIMECOL=> DESCRIPTOR(order_timestamp),
SIZE=> INTERVAL '60' SECOND)
) 
group by window_start,window_end,window_time


-- Remeber window is only emitted once watermark passes window end.
--Watermark= max_event_time - 30 desc

-- also the op is an append only stream, verify using chanleog display

--------------------------------------------------------------------------------------------------------------------------------------

-- Processing Time (wall clock)

-- no need for window as processing time is always linear and progressing, so there wont be any late event


CREATE TABLE orders_processing_time (
    order_id STRING,
    order_amount DOUBLE,
    product_id STRING,
    customer_id STRING,
    order_process_time as PROCTIME()
)
WITH (
 'connector' = 'faker',
 'rows-per-second' = '1',

 'fields.order_id.expression' = '#{regexify ''ord-[0-9]{5}''}',
 'fields.order_amount.expression' = '#{number.randomDouble ''2'',''100'',''5000''}',
 'fields.product_id.expression' = '#{regexify ''prod-[0-9]{4}''}',
 'fields.customer_id.expression' = '#{regexify ''cust-[0-9]{3}''}'

);


-- query

select 
window_start,window_end,sum(order_amount) as revenue,window_time
FROM TABLE(
TUMBLE(DATA=> TABLE orders_processing_time,
TIMECOL=> DESCRIPTOR(order_process_time),
SIZE=> INTERVAL '60' SECOND)
) 
group by window_start,window_end,window_time;

-- immediate window emitted as soon as processing time crosses window

-- also the op is an append only stream, verify using chanleog display




