-- The TABLE() function basically puts each event under a window generated based on time column in DESCRIPTOR. Then we group based on start and end time,
-- to compute analytics



-- tumbing window (fixed non overlapping window)  
-- window emitted only when watermark>=window_end time
-- produces an append only changelog

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
 'rows-per-second' = '100',

 'fields.order_id.expression' = '#{regexify ''ord-[0-9]{5}''}',
 'fields.order_amount.expression' = '#{number.randomDouble ''2'',''100'',''5000''}',
 'fields.product_id.expression' = '#{regexify ''prod-[0-9]{4}''}',
 'fields.customer_id.expression' = '#{regexify ''cust-[0-9]{3}''}',

 'fields.order_timestamp.expression' ='#{date.past ''1'',''SECONDS''}'
);

SET 'sql-client.execution.result-mode' = 'changelog';  -- to visulaize insert only stream


-- we can also write to a flink table backed by kafka topic if reqd.


-- the order_timestamp is generated based on 1 sec before current time. The watermark is 30 sec, so if current time 
-- 7:00:02 am then  faker will generate rows for 7:00:01 am   and the window will be 7:00:00 am - 7:00:30am. Onky when there is an event
--  with timestamp 7:01:00 (so current timestamp needs to be 7:01:01 ) the state is cleared from backend and window (7:00:00 - 7:00:30) is emitted.

select 
window_start,window_end,sum(order_amount) as revenue,window_time
FROM TABLE(
TUMBLE(DATA=> TABLE orders,
TIMECOL=> DESCRIPTOR(order_timestamp),
SIZE=> INTERVAL '30' SECOND)
) 
group by window_start,window_end,window_time;




-- sliding/hop window (overlapping window)  
-- behaves like tumbling when slide=size
-- window emitted only when watermark>=window_end time
-- produces an append only changelog

select 
window_start,window_end,sum(order_amount) as revenue,window_time
FROM TABLE(
HOP(DATA=> TABLE orders,
TIMECOL=> DESCRIPTOR(order_timestamp),
SLIDE=> INTERVAL '1' SECONDS,
SIZE=> INTERVAL '30' SECOND)
) 
group by window_start,window_end,window_time;


-- cumulate windows ( non overlapping)
-- window emitted only when watermark>=window_end time
-- produces an append only changelog.

-- Remeber here each cumulate row in a window is emitted as soon as the watermark reachs that window'end. It does NOT wait for entire 30 sec to complete  

select 
window_start,window_end,sum(order_amount) as revenue,window_time
FROM TABLE(
CUMULATE(DATA=> TABLE orders,
TIMECOL=> DESCRIPTOR(order_timestamp),
STEP=> INTERVAL '10' SECONDS,
SIZE=> INTERVAL '30' SECOND)
) 
group by window_start,window_end,window_time;


-- session windows ( dynamic non overlapping)
-- produces append mode
-- these get grouped naturally not on fixed time
-- remeber here window_end= last_event+ session_duration - and window emist when watermark>=session_duration


example is in ipad note
