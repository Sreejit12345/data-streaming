# reference :https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
# https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html?session_ref=https%3A%2F%2Fwww.google.com%2F&url_ref=https%3A%2F%2Fdocs.confluent.io%2Fkafka-clients%2Fpython%2Fcurrent%2Foverview.html#

# to do this we first created a topic "thermostat_readings" with 6 partitions and produced some messages to it via UI and terminal

import argparse,configparser
from confluent_kafka import Consumer

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
configuration['group.id'] = config['consumer']['group.id']
configuration['auto.offset.reset'] = config['consumer']['auto.offset.reset']

print(configuration)

consumer = Consumer(configuration)


topic = "thermostat_readings"
consumer.subscribe([topic])

#Set subscription to supplied list of topics.


# remember the message reads will be incremental from offsets



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

except KeyboardInterrupt:
    pass

finally:
    consumer.close()

print(consumer)