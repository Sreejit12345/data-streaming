# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?session_ref=https%3A%2F%2Fwww.google.com%2F&url_ref=https%3A%2F%2Fdocs.confluent.io%2Fkafka-clients%2Fpython%2Fcurrent%2Foverview.html#pythonclient-producer


# to do this we first created a topic "thermostat_readings" with 6 partitions

# this produces messages with schema, so the consumer must have a deserializer to read it

import argparse,configparser
from confluent_kafka import Producer
import json
import random
import time
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext,MessageField


def create_random_message():

    sensor_id=random.randint(100,500)
    location=random.choice(['living_room','kitchen','bedroom','bathroom'])
    temperature=random.randint(18,30)
    read_at=int(time.time()*1000)

    message={
        'sensor_id':sensor_id,
        'location':location,
        'temperature':temperature,
        'read_at':read_at
    }

    return message



parser = argparse.ArgumentParser()
parser.add_argument('config_file')  #means the 1st positional file name will be the path for config file (args.config_file)
args = parser.parse_args()  #args contains the parsed arguments, args.config_file gives the config file path


# now we read the ini file to get the consumer configuration

config = configparser.ConfigParser()

config.read(args.config_file)

configuration={}

configuration['bootstrap.servers'] = config['default']['bootstrap.servers']
configuration['security.protocol'] = config['default']['security.protocol']
configuration['sasl.mechanisms'] = config['default']['sasl.mechanisms']
configuration['sasl.username'] = config['default']['sasl.username']
configuration['sasl.password'] = config['default']['sasl.password']


print(configuration)

producer = Producer(configuration)
topic = "thermostat_readings"

# passsing the schema registry configuration, this key is different from consumer configuration and is set in portal.
schema_registry_conf={}
schema_registry_conf['url']=config['schema-registry']['url']
schema_registry_conf['basic.auth.user.info']=config['schema-registry']['basic.auth.user.info']

scl=SchemaRegistryClient(schema_registry_conf)

# we get schema_id and subject_name from schema registry portal. This returns the schema object
schema_obj=scl.get_schema(schema_id='100003',subject_name='thermostat_readings-value')  #this gets the schema object

serializer=JSONSerializer(schema_str=schema_obj.schema_str,schema_registry_client=scl)


num_messages=10
msg_count=0

while(msg_count<num_messages):

    message=create_random_message()

    print(f'producing message: {message}')

    val=serializer.__call__(message,SerializationContext(topic,MessageField.VALUE))

    producer.produce(topic, value=val, key=str(message['sensor_id']))   #produces message to topic, value must be string or bytes

    producer.flush()   #waits all messages in the Producer queue to be delivered.

    msg_count+=1

    time.sleep(1)



