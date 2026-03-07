# reference :https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?session_ref=https%3A%2F%2Fwww.google.com%2F&url_ref=https%3A%2F%2Fdocs.confluent.io%2Fkafka-clients%2Fpython%2Fcurrent%2Foverview.html#

# to do this we first created a topic "thermostat_readings" with 6 partitions and produced some messages to it via UI and terminal
# we also added a schema to the value of the message , so we need a deserializer to decode it
# we used a json schema so we will use a json deserializer

# if i have to consumer messages with schema the producer must have produced messages with schema too

# instead of deserializing to a simple dict, we will deserialize to a complex object of a class, using the from_dict callback

'''
# Basic installation
pip install confluent-kafka

# With Schema Registry support
pip install "confluent-kafka[avro,schemaregistry]"     # Avro
pip install "confluent-kafka[json,schemaregistry]"     # JSON Schema
pip install "confluent-kafka[protobuf,schemaregistry]" # Protobuf

# With Data Contract rules (includes CSFLE support)
pip install "confluent-kafka[avro,schemaregistry,rules]"

'''

import argparse,configparser
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext,MessageField
from dataclasses import dataclass

@dataclass
class ThermostatMessage:
    sensor_id: int
    location: str
    temperature: float
    read_at: int

parser = argparse.ArgumentParser()
parser.add_argument('config_file')  #means the 1st positional file name will be the path for config file (args.config_file)
args = parser.parse_args() #args contains the parsed arguments, args.config_file gives the config file path


# now we read the ini file to get the consumer configuration
config = configparser.ConfigParser()

config.read(args.config_file)

topic = "thermostat_readings"

configuration={}

configuration['bootstrap.servers'] = config['default']['bootstrap.servers']
configuration['security.protocol'] = config['default']['security.protocol']
configuration['sasl.mechanisms'] = config['default']['sasl.mechanisms']
configuration['sasl.username'] = config['default']['sasl.username']
configuration['sasl.password'] = config['default']['sasl.password']
configuration['group.id'] = config['consumer']['group.id']
configuration['auto.offset.reset'] = config['consumer']['auto.offset.reset']


consumer = Consumer(configuration)


def dict_to_thermostat_message(data:dict,ctx):

    '''
    function to convert dict to ThermostatMessage object
    '''

    if data is None:
        return None

    return ThermostatMessage(
        sensor_id=data['sensor_id'],
        location=data['location'],
        temperature=data['temperature'],
        read_at=data['read_at']
    )

# passsing the schema registry configuration, this key is different from consumer configuration and is set in portal.
schema_registry_conf={}
schema_registry_conf['url']=config['schema-registry']['url']
schema_registry_conf['basic.auth.user.info']=config['schema-registry']['basic.auth.user.info']

scl=SchemaRegistryClient(schema_registry_conf)

# we get schema_id and subject_name from schema registry portal. This returns the schema object
schema_obj=scl.get_schema(schema_id='100003',subject_name='thermostat_readings-value')  #this gets the schema object

# we create a json deserializer object
j_cl=JSONDeserializer(schema_registry_client =scl,schema_str=schema_obj, from_dict=dict_to_thermostat_message)
# from_dict is a callback function to convert dict to an object

consumer.subscribe([topic])

#Set subscription to supplied list of topics.


# remember the message reads will be incremental from offsets- in case any error occurs to deserialize a message, it skips to next message



'''

In Kafka, each message in a partition has a unique offset, and consumers track which offsets they have read.
By default, Kafka can automatically commit offsets at regular intervals, but applications can also use manual commits to control 
exactly when a message is considered processed.
 When a consumer commits an offset—whether automatically or manually—the offset is stored in Kafka’s internal topic __consumer_offsets 
 for that consumer group. This allows the consumer, even after a crash or restart, 
to resume reading from the correct position in each partition, ensuring no messages are skipped or unnecessarily reprocessed.

'''

try:

    while True:
        msg=consumer.poll(1.0)   #consumer.poll() consumes a single message
        # 1.0 is the maximum time to block waiting for message

        if(msg is None):
            print("Waiting...")

        elif(msg.error()):
            print("ERROR: %s".format(msg.error()))
        
        else:
            print("Consumed event from topic {topic}: key = {key} value = {value}".format(topic=msg.topic(), key=msg.key(), value=msg.value()))
            print("val is" , j_cl.__call__(data=msg.value(),ctx=SerializationContext(topic=topic,field=MessageField.VALUE)))
except KeyboardInterrupt:
    pass

finally:
    consumer.close()
