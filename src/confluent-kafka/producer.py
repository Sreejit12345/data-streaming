# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?session_ref=https%3A%2F%2Fwww.google.com%2F&url_ref=https%3A%2F%2Fdocs.confluent.io%2Fkafka-clients%2Fpython%2Fcurrent%2Foverview.html#pythonclient-producer


# to do this we first created a topic "thermostat_readings" with 6 partitions

# remeber this produces a basic message without any schema, so the consumer must read it as is without any deserializer



import argparse,configparser
from confluent_kafka import Producer
import json
import random
import time


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

num_messages=10
msg_count=0

while(msg_count<num_messages):

    message=create_random_message()

    print(f'producing message: {message}')

    producer.produce(topic, value=json.dumps(message), key=str(message['sensor_id']))   #produces message to topic, value must be string or bytes

    producer.flush()   #waits all messages in the Producer queue to be delivered.

    msg_count+=1

    time.sleep(1)



