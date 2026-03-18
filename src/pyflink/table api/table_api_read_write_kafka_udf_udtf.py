# we will read from kafka topic, transform to row by udf and udtf, and write to another kafka topic
# we will also use map and flatmap to transform the data


# Remeber we need to add these 2 jars in flink home- we can get tthe home from find_flink_home.py script
# 1. flink-connector-kafka-4.0.1-2.0.jar       https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/4.0.1-2.0
# 2. flink-sql-connector-kafka-3.2.0-1.18.jar   https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/kafka/#dependency

# we set up the kafka topic using docker image and produced some data to the topic.
#https://hub.docker.com/r/apache/kafka
# remeber to map container port to host port when running the docker image, e.g. -p 9092:9092 for kafka

#https://nightlies.apache.org/flink/flink-docs-release-2.2/api/python/reference/pyflink.table/index.html

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf,udtf
from pyflink.common import Row
from pyflink.table.types import DataType

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


# 2. create source Table
table_env.execute_sql("""
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
'topic'='orders_new',
'properties.bootstrap.servers'='localhost:9092',
'format'='json',
'scan.startup.mode'='earliest-offset'
)
;
""")

source_table=table_env.from_path("orders_ip")


# remeber row is a container for multiple fields, 
# and we need to specify the field names and types in the udf result type, e.g. Row<order_id STRING, product_id STRING, order_amount DOUBLE>.
#ultimately the get treated as different columns in the table after the udf transformation, and we can select them by their names in the downstream operations.
# here we get 3 fields as result of of the udf- order_id and product_id and order_amount, which are extracted from order_id and product_id and order_amount respectively, and we can use them as separate columns in the downstream operations.
@udf(result_type='Row<order_id STRING, product_id STRING, order_amount DOUBLE>')
def generate_product_order_number(order_id: str, product_id: str, order_amount: float) -> Row:

    '''
    1:1 - ip 1 row = op 1 row
    '''

    order_number= order_id.split("-")[-1] # extract the number part from order id, e.g. order-1234 -> 1234
    product_number= product_id.split("-")[-1] # extract the number part from product id, e.g. product-5678 -> 5678      
    
    return Row(order_number, product_number,order_amount)



# if order_id does not have comma just extract the order number and product number and order amount, 
simple_orders=source_table.filter( ~col('order_id').like('%,%')).map(generate_product_order_number(col("order_id"),col("product_id"), col("order_amount")))
# map is similar to select in table api.




# if order_id has comma then we need to explode the order and create multiple rows for each order,
#  e.g. order-1234,order-5678 -> 2 rows with order-1234 and order-5678 respectively, and we can use flatmap for this.

@udtf(result_types=['STRING', 'STRING', 'DOUBLE'])
def explode_orders(order_id: str, product_id: str, order_amount: float):
    '''
    1:n - ip 1 row = op n row
    '''
    
    # assumption the number of commas in order_id,product_id and amount are same, e.g. order-1234,order-5678 and product-5678,product-9012 and 100.0
    # the amount is derived by dividing with number of orders
    order_ids= order_id.split(",") # split the order id by comma, e.g. order-1234,order-5678 -> [order-1234, order-5678]
    product_ids= product_id.split(",") # split the product id by comma, e.g. product-5678,product-9012 -> [product-5678, product-9012]
    order_amount_individual=order_amount/len(order_ids) # divide the order_amount by number of orders, e.g. 100.0/2 -> 50.0 for each order

    order_amount_arr=[order_amount_individual]*len(order_ids) # create an array of order amount for each order, e.g. [50.0, 50.0] for 2 orders
    
    for i in range(len(order_ids)):
        yield Row(order_ids[i], product_ids[i], order_amount_individual)  #essentially each element of list becomes a row

complex_orders=source_table.filter( col('order_id').like('%,%')).flat_map(explode_orders(col("order_id"), col("product_id"), col("order_amount")))

all_orders=simple_orders.union_all(complex_orders)

#all_orders.execute().print()

'''
Sample json

{"order_id":"ORD-1001,ORD-12","order_amount":200.00,"product_id":"PROD-501,PROD-502","customer_id":"CUST-301","order_timestamp":"2026-03-08 14:23:45.123"}
{"order_id":"ORD-1002","order_amount":499.50,"product_id":"PROD-502","customer_id":"CUST-302","order_timestamp":"2026-03-08 14:24:12.456"}

OP:
+------------+------------+--------------+
|order_number|prod_number |order_amount  |
+------------+------------+--------------+
|1001        |501         |100.0         |
|12          |502         |100.0         |
|1002        |502         |499.5         |


'''

#create sink Table
table_env.execute_sql("""
 CREATE TABLE orders_op
(
    order_id STRING,
    product_id STRING,
    order_amount DOUBLE
)
WITH
( 
'connector' ='kafka',
'topic'='orders_op',
'properties.bootstrap.servers'='localhost:9092',
'format'='json'
)
;
""")

sink_table=table_env.from_path("orders_op")

all_orders.select(col('order_id'), col('product_id'), col('order_amount')).execute_insert("orders_op").wait()

# observer how the messages get wriiten to the output topic in kafka, e.g. using kafka-console-consumer.sh script, 
# and we can see the transformed messages with order_id and product_id and order_amount as separate fields in the output topic.