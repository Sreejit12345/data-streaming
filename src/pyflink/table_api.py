from pyflink.table import EnvironmentSettings,TableEnvironment

#env_settings=EnvironmentSettings.in_streaming_mode()    -- for streaming mode

env_settings=EnvironmentSettings.in_streaming_mode()

t_env = TableEnvironment.create(env_settings)


my_source_ddl = """
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen',
  'rows-per-second'= 500
)

"""

t_env.execute_sql(my_source_ddl)