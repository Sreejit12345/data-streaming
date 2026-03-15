import os
from pyflink.find_flink_home import _find_flink_home

'''
Very useful to find out flink home and where to place the JARs for pyflink to work. 
You can run this script as a standalone script to find out the FLINK_HOME and the lib directory where you should place your JARs.

JARS are required for external connectors like Kafka, JDBC, etc.
 If you are using the datagen connector and print connector as shown in the table_api_basic_structure.py, you don't need to place any JARs as these connectors are included in the Flink distribution by default.
'''

flink_home = _find_flink_home()


if flink_home:
    print(f"Success! FLINK_HOME is: {flink_home}")
    
    
    lib_dir = os.path.join(flink_home, "lib")
    print(f"You should place your JARs in: {lib_dir}")
else:
    print("Error: Could not find a valid Flink distribution.")