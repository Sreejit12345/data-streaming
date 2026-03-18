

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

#https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/tableapi/
#https://nightlies.apache.org/flink/flink-docs-release-2.2/api/python/reference/pyflink.table/index.html

# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


# 2. create source Table
table_env.execute_sql("""
    CREATE TABLE datagen (
        id INT,
        data STRING
    ) WITH (
        'connector' = 'datagen',
        'fields.id.kind' = 'sequence',
        'fields.id.start' = '1',
        'fields.id.end' = '100',
        'fields.data.kind' = 'random'
    )
""")

source_table = table_env.from_path("datagen")

transform1=source_table.add_columns( (col("id") + 1).alias("id_plus_one") ).\
filter(col("id") %2 !=0)

transform2=transform1.group_by(col("data")).select(col("data"), col("id").sum.alias("cnt"))

transform2.execute().print()