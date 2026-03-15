
# add the flink faker connector jar to the classpath, also add the jar to the lib directory
# download jar from https://flink-packages.org/packages/flink-faker
# use find_flink_home.py to find out where to place the jar
# remeber there might be dependency issues, so might fail with certain versions


from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/python/table/intro_to_table_api/


# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.get_config().set(
    "pipeline.jars", 
    "file:///home/proschakraborty/data-streaming/.venv/lib/python3.9/site-packages/pyflink/lib/flink-faker-0.5.3.jar"
)

# 2. create source Table
table_env.execute_sql("""
    
CREATE  TABLE heros (
  `name` STRING,
  `power` STRING, 
  `age` INT
) WITH (
  'connector' = 'faker', 
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.power.null-rate' = '0.05',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
);

""")

source_table = table_env.from_path("heros")

source_table.execute().print()