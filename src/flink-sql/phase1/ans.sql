-- create a basic table with faker data


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
'number-of-rows'='500',
'rows-per-second'='100',
'fields.user_id.expression' = '#{numerify ''user_##''}',
'fields.name.expression' = '#{GreekPhilosopher.name}',
'fields.country.expression' = '#{Options.option ''India'', ''Usa'', ''Australia'')}',
'fields.signup_time.expression' = '#{date.past ''5'',''1'',''SECONDS''}'
)


-- select columns

select user_id,signup_time from Users

-- sort by time 

--1) We need to have a time attribute defined in order to sort. SO we add a watermark ( event time)

ALTER TABLE Users ADD WATERMARK FOR `signup_time` as `signup_time` - INTERVAL '5' second;
select * from Users order by signup_time;

--2) We can also add a column for processing time not event time

ALTER TABLE Users ADD `proc_time` AS PROCTIME();
select * from Users order by proc_time;


-- Aggregate 
Produces an update stream as result.

select count(*), country
from Users group by country