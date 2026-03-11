-- simple stream join without watermark
--- unbounded state-- if TTL not configured will fail eventually)

-- if i/p is updating join also updates historical records . if i/p is strictly append then inner join produces append.
-- INNER only emits rows when there is a match

-- for left / right / outer if no matches are found - the join emits null - if values are matched later- then join updates those values

-- chk ipad notes


'''

1️⃣ INNER JOIN (no watermark)

Only emits a row when a match exists.

Append-only output if both inputs are append-only. If table is updating- update stream produced and all recs updated.

Duplicates: If multiple rows in either stream match the same key, Flink emits a row for each combination, but does not update previous rows.

State: stores all historical rows to match future arrivals → can grow unbounded.

Example:

Orders	Payments	Output
o1	p10	o1,p10
	p11	o1,p11

No retractions happen.

2️⃣ LEFT JOIN (no watermark)

Emits a row for every row on the left.

If no matching right row exists yet, Flink must emit a provisional row with NULLs.

Later, if a matching right row arrives:

Flink retracts the old NULL row

Emits the proper join.

Output is always a retract/update stream, even if inputs are append-only.

Example:

Orders	Payments	Output (provisional / final)
o1	none	(o1, NULL)
	p10	retract (o1, NULL), emit (o1, p10)
3️⃣ RIGHT JOIN (no watermark)

Symmetric to LEFT JOIN, but for right side.

Emits provisional NULL rows for unmatched right rows.

Updates occur when the matching left row eventually arrives.

4️⃣ FULL OUTER JOIN (no watermark)

Combines LEFT + RIGHT logic.

All unmatched rows are initially emitted with NULLs.

Updated rows are emitted when the missing side arrives.

Output is always a retract/update stream.

State requirement is heaviest because Flink must remember all unmatched rows from both sides.

'''


CREATE TABLE orders_stream (
  order_id STRING,
  user_id STRING,
  amount DOUBLE,
  order_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
 'fields.order_id.expression'='#{number.numberBetween ''1'',''100''}',
  'fields.user_id.expression' = '#{Internet.uuid}',
  'fields.amount.expression' = '#{Number.randomDouble ''2'',''10'',''500''}',
  'fields.order_time.expression' = '#{date.past ''1'',''SECONDS''}'
);


CREATE TABLE payments_stream (
  payment_id STRING,
  order_id STRING,
  payment_method STRING,
  payment_time TIMESTAMP(3)
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.payment_id.expression' = '#{Internet.uuid}',
   'fields.order_id.expression'='#{number.numberBetween ''1'',''100''}',
  'fields.payment_method.expression' = '#{options.option ''CARD'',''UPI'',''NETBANKING''}',
  'fields.payment_time.expression' = '#{date.past ''1'',''SECONDS''}'
);


-- for inner only produces rows for matching recs only- also since source is append only,  tgt is append only too , but duplicaes might exiist if 1 key matches 
-- multiple in other table
select o.order_id,p.payment_method,p.payment_time from orders_stream o 
join payments_stream p 
on(o.order_id=p.order_id);

-- for left/right/outer only produces all rows immediately- if no mathch then gives null. When match comes, prev row is retracted and new row comes (update basically)
select o.order_id,p.payment_method,p.payment_time from orders_stream o 
left join payments_stream p 
on(o.order_id=p.order_id);




-- with watermarks (if the event time is not included in the join cond somehow- watermarks dont help because it still has to retain state)

-- with watermarks (if event time is included it becomes an interval join)

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

-- interval join
--(an interval join requires at least one equi-join predicate and a join condition that bounds the time on both sides)
-- interval join only supports append-only tables with time attributes. Since time attributes are quasi-monotonic increasing, 
--Flink can remove old values from its state without affecting the correctness of the result.


-- Remeber in this case, as soon as cond matches rows are emitted but state is cleared as soon as watermark reaches condition. 

CREATE TABLE orders_stream (
  order_id STRING,
  user_id STRING,
  amount DOUBLE,
  order_time TIMESTAMP(3),

    WATERMARK FOR order_time AS order_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
 'fields.order_id.expression'='#{number.numberBetween ''1'',''100''}',
  'fields.user_id.expression' = '#{Internet.uuid}',
  'fields.amount.expression' = '#{Number.randomDouble ''2'',''10'',''500''}',
  'fields.order_time.expression' = '#{date.past ''1'',''SECONDS''}'
);


CREATE TABLE payments_stream (
  payment_id STRING,
  order_id STRING,
  payment_method STRING,
  payment_time TIMESTAMP(3),
  WATERMARK FOR payment_time AS payment_time - INTERVAL '30' SECOND
) WITH (
  'connector' = 'faker',
  'rows-per-second' = '1',
  'fields.payment_id.expression' = '#{Internet.uuid}',
   'fields.order_id.expression'='#{number.numberBetween ''1'',''100''}',
  'fields.payment_method.expression' = '#{options.option ''CARD'',''UPI'',''NETBANKING''}',
  'fields.payment_time.expression' = '#{date.past ''1'',''SECONDS''}'
);

SET 'execution.checkpointing.interval' = '1min';

SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

select o.order_id,p.payment_method,p.payment_time from orders_stream o 
join payments_stream p 
on(o.order_id=p.order_id)
AND o.order_time BETWEEN 
                p.payment_time  AND p.payment_time + INTERVAL '5' MINUTE


-- we assume payments happen logically before orders
-- this allows to clear state so ideally if a payment happens at 10:00:05 then order should come b/w 
-- 10:00:05 and 10:05:05. Now if we have this in the join , flink knows that for a payment at 10:00:05, it has to keep state until
-- 10:05:05 for the corresponding order.

'''


order comes in at 10:05:01- 
now , we only care about payments that came b/w 10:00:01 and 10:05:01
so when the combined minimum watermark of join >=10:05:01   -- we can safely and clear state


Remember as and when new rows come for orders and payments their indicidual watermarks progress,but the combined is always min one
'''

'''
checkpoint size in UI with interval join ( see how size is consistent and soen not increase)

ID	Status	Acknowledged	Trigger Time	Latest Acknowledgement	End to End Duration	Checkpointed Data Size 	Full Checkpoint Data Size	Processed (persisted) in-flight data
5	
COMPLETED
3/3	2026-03-11 23:08:12.634	2026-03-11 23:08:13.339	705ms	25.6 KB	25.6 KB	0 B (0 B)
4	
COMPLETED
3/3	2026-03-11 23:07:12.651	2026-03-11 23:07:13.649	998ms	25.4 KB	25.4 KB	0 B (0 B)
3	
COMPLETED
3/3	2026-03-11 23:06:12.668	2026-03-11 23:06:13.354	686ms	25.6 KB	25.6 KB	0 B (0 B)
2	
COMPLETED
3/3	2026-03-11 23:05:12.688	2026-03-11 23:05:14.764	2s 76ms	26.1 KB	26.1 KB	0 B (0 B)
1	
COMPLETED
3/3	2026-03-11 23:04:12.849	2026-03-11 23:04:16.371	3s 522ms	22.4 KB	22.4 KB	0 B (0 B)
'''


'''
checkpoint size in UI without interval join ( see how size is increasing constantly)

	11	
COMPLETED
3/3	2026-03-11 23:23:40.994	2026-03-11 23:23:41.444	450ms	41.7 KB	41.7 KB	0 B (0 B)
10	
COMPLETED
3/3	2026-03-11 23:22:41.008	2026-03-11 23:22:41.402	394ms	39.0 KB	39.0 KB	0 B (0 B)
9	
COMPLETED
3/3	2026-03-11 23:20:56.400	2026-03-11 23:20:56.951	551ms	37.6 KB	37.6 KB	0 B (0 B)
8	
COMPLETED
3/3	2026-03-11 23:19:56.433	2026-03-11 23:19:56.816	383ms	34.9 KB	34.9 KB	0 B (0 B)
7	
COMPLETED
3/3	2026-03-11 23:18:56.451	2026-03-11 23:18:56.903	452ms	32.1 KB	32.1 KB	0 B (0 B)
6	
COMPLETED
3/3	2026-03-11 23:17:56.465	2026-03-11 23:17:56.813	348ms	29.3 KB	29.3 KB	0 B (0 B)
5	
COMPLETED
3/3	2026-03-11 23:16:56.488	2026-03-11 23:16:56.812	324ms	26.4 KB	26.4 KB	0 B (0 B)
4	
COMPLETED
3/3	2026-03-11 23:15:56.505	2026-03-11 23:15:56.804	299ms	23.7 KB	23.7 KB	0 B (0 B)
3	
COMPLETED
3/3	2026-03-11 23:14:56.523	2026-03-11 23:14:56.893	370ms	20.5 KB	20.5 KB	0 B (0 B)
2	
COMPLETED
3/3	2026-03-11 23:13:56.542	2026-03-11 23:13:58.660	2s 118ms	16.5 KB	16.5 KB	41 B (0 B)


