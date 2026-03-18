# we will create table using table descriptor 
# and specify the watermark strategy in the table descriptor, and then we can use the event time attribute for time-based operations,
#  e.g. windowing, and the watermark will help to handle late events and trigger the window computations at the right time.

#https://nightlies.apache.org/flink/flink-docs-release-2.2/api/python/reference/pyflink.table/index.html

#https://nightlies.apache.org/flink/flink-docs-master/api/python/reference/pyflink.table/api/pyflink.table.Table.join_lateral.html

from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor
from pyflink.table.udf import udf,udtf
from pyflink.common import Row
from pyflink.table.expressions import col,lit
from pyflink.table.window import Tumble

# create a stream TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


table_env.get_config().set(
    "table.exec.source.idle-timeout",
    "10s"   # to handle idle partitions and allow watermark progess
)

# we can also create by ddl but  creating via table api here:
table_env.create_table(
    'table_1',
    TableDescriptor.for_connector('kafka')
        .schema(Schema.new_builder()\
                .column('id', DataTypes.STRING())\
                .column('data', DataTypes.STRING())\
                .column('created_ts',DataTypes.TIMESTAMP(3))\
                .watermark('created_ts',"created_ts - INTERVAL '5' SECOND")\
                .build())\
        .option('connector', 'kafka')\
        .option('topic', 'sample_topic123')\
        .option('properties.bootstrap.servers', 'localhost:9092')\
        .option('format', 'json')\
        .option('scan.startup.mode', 'latest-offset')\
        .build())


# now we do word count and group the count of words every minute, so each minute the count of each word

# the topic receives json schema events like:

'''

{"id":"1","data":"hello this is sreejit","created_ts":"2026-03-18 10:15:30.123"}
{"id":"2","data":"sreejit is a married to arshia","created_ts":"2026-03-18 10:15:49.456"}
{"id":"3","data":"sreejit works in microsoft","created_ts":"2026-03-18 10:16:06.789"}   -- this events emits window from 10:15:00 to 10:16:00
{"id":"4","data":"sreejit logged in from mobile device","created_ts":"2026-03-18 10:18:20.001"}  -- this events emits window from 10:16:00 to 10:17:00
{"id":"5","data":"retry attempt succeeded after network timeout.","created_ts":"2026-03-18 10:15:55.999"} -- data dropped nothing happends

and so on....
'''



@udtf(result_types=['STRING'])
def explode_words(data):
    data_arr=data.split(' ')

    for i in data_arr:
        yield Row(i)

src_table=table_env.from_path('table_1')


#exploded=src_table.flat_map(explode_words(col('data')))
# if we execute above we get a single column with each array element being a word
# even if u use creates_ts in udtf it will loose the watermark property
# but we need to get the created_ts since its the watermark column- so we use LATERAL JOIN

'''
Lateral join

A lateral join in Flink SQL is a type of join that allows you to apply a table-valued function to each row of a table and
generate additional rows based on the function's output. Lateral joins are useful for scenarios
where you need to split a column into multiple rows or generate additional rows based on complex calculations or queries.


Similar to flatMap:

Key Differences
Feature 	flat_map	join_lateral
Output Columns	Returns only the columns produced by the UDTF.	Returns columns from both the original table and the UDTF.
Relational Model	Acts as a transformation/flattening operator.	Acts as a join between a table and a function's result.
Column Preservation	Original columns (like created_ts) are lost unless manually re-emitted by the UDTF.	Original columns are preserved and can be selected immediately.
Filtering Behavior	If the UDTF returns nothing, the row is dropped.	If the UDTF returns nothing, the row is dropped (unless using left_outer_join_lateral).

'''

op=src_table.join_lateral(explode_words(col('data')).alias('word')).select(col('created_ts'),col('word'))

# if udtf returns multi column then we pass those in alias and it is picked up by position.


'''

After lateral join we get exploded data like this, where created_ts is same

+----+-------------------------+--------------------------------+
| op |              created_ts |                           word |
+----+-------------------------+--------------------------------+
| +I | 2026-03-18 10:15:30.123 |                          hello |
| +I | 2026-03-18 10:15:30.123 |                           this |
| +I | 2026-03-18 10:15:30.123 |                             is |
| +I | 2026-03-18 10:15:30.123 |                        sreejit |

'''


result = op.window(
    Tumble.over(lit(1).minute)
          .on(col('created_ts'))
          .alias('w')
).group_by(
    col('word'),
    col('w')
).select(
    col('w').start.alias('start_time'),
    col('w').end.alias('end_time'),
    col('word'),
    col('word').count.alias('word_count')
)


result.execute().print()
