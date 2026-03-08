-- Set mode to batch

SET 'execution.runtime-mode' = 'batch';

-- read from orders.csv (remember no headers)
-- We need to copy this file to docker container. Find the name of the taskmanager container and substitue in cmd
-- "setup-taskmanager-1"   is the name of my docker container. This can be found in docker desktop.

-- docker cp "C:\Users\SREEJIT\OneDrive\Desktop\data-streaming\src\flink-sql\phase2\orders.csv" setup-taskmanager-1:/tmp/orders.csv

-- test if file exists:

--docker exec -it setup-taskmanager-1 ls /tmp/orders.csv

CREATE TABLE orders (
    order_id STRING,
    user_id STRING,
    amount DOUBLE,
    order_time STRING,
    created_ts as PROCTIME() 
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/orders.csv',
    'format' = 'csv'
);

-- high orders
select * from orders where amount>100;


-- top 10 orders  (in batch mode we get exactly what we expect 10 orders)  - This reads the entire file as batch and produces 1 op
select * from
 (
 select *, row_number() over(order by amount desc) rn from orders
) q
 where rn<=10;



-- write results back to a csv sink (append only)

CREATE TABLE csv_sink (
  order_id STRING,
  amount DOUBLE,
  rn BIGINT
) WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/top_order.csv',
  'format' = 'csv'
);

insert into csv_sink
select order_id,amount,rn from
 (
 select *, row_number() over(order by amount desc) rn from orders
) q
 where rn<=10;

docker exec -it setup-taskmanager-1 ls /tmp/top_order.csv ; -- to see the file that was written to sink , sopt file name from here

docker exec -it setup-taskmanager-1 cat /tmp/top_order.csv/part-509aaac2-1e4f-42b0-939a-8b4874a8d44f-task-0-file-0



---------------------------------------------------------------------------------------------------------------------------------------------



-- If we change to streaming - the behavior changes and it constantky updates the rn as new records are streamed.

-- top 1 order

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';  -- to visulaize updates

select * from
 (
 select *, row_number() over(order by amount desc) rn from orders
) q
 where rn=1;

 '''
 +I                        ord_101                           u_1                          10.5            2026-03-08 10:00:00 2026-03-08 05:20:04.184
 -U                        ord_101                            u_1                           10.5            2026-03-08 10:00:00 2026-03-08 05:20:04.194
 +U                        ord_102                            u_2                           25.0            2026-03-08 10:00:05 2026-03-08 05:20:04.194
 -U                        ord_102                            u_2                           25.0            2026-03-08 10:00:05 2026-03-08 05:20:04.195
 +U                        ord_103                            u_3                          100.0            2026-03-08 10:01:00 2026-03-08 05:20:04.195
 -U                        ord_103                            u_3                          100.0            2026-03-08 10:01:00 2026-03-08 05:20:04.195
 +U                        ord_111                            u_6                          120.0            2026-03-08 10:30:00 2026-03-08 05:20:04.195
 -U                        ord_111                            u_6                          120.0            2026-03-08 10:30:00 2026-03-08 05:20:04.195
 +U                        ord_115                            u_7                          200.0            2026-03-08 11:00:00 2026-03-08 05:20:04.195
 -U                        ord_115                            u_7                          200.0            2026-03-08 11:00:00 2026-03-08 05:20:04.195
 +U                        ord_124                            u_9                          400.0            2026-03-08 13:00:00 2026-03-08 05:20:04.195

'''


-- the 1st order definition keeps changing as new rows get added.





-- to further see the diff b/w batch and streaming we can use faker

CREATE TABLE Users
(
    user_id STRING,
    name STRING,
    country STRING,
    signup_time TIMESTAMP(3)
)
WITH
(
'connector' ='faker',
'number-of-rows'='500000',
'rows-per-second'='50000',
'fields.user_id.expression' = '#{numerify ''user_##''}',
'fields.name.expression' = '#{GreekPhilosopher.name}',
'fields.country.expression' = '#{Options.option ''India'', ''Usa'', ''Australia'')}',
'fields.signup_time.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
)

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'table'; 

select count(*),country from Users group by country;
-- in batch mode it waits for faker to generate all rows then produces results



SET 'execution.runtime-mode' = 'streaming';

select count(*),country from Users group by country;
-- in streaming it continously updates the count.
