from confluent_kafka.admin import AdminClient,NewTopic,ConfigResource
import argparse,configparser


# remeber all thr admin apis are aysnc and returns a future object which can be used to track the progress of the operation

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


# search for a topic in the cluster

topic_to_search="admin_test_topic"
topic_exists=False

admin_cl=AdminClient(configuration)

all_topics=admin_cl.list_topics()

for i in all_topics.topics.keys():
    if(i==topic_to_search):
        print(f'Topic {topic_to_search} found in the cluster')
        topic_exists=True
        break
    
if(topic_exists==False):
    print(f'Topic {topic_to_search} not found in the cluster')

# create new topic if not exists

if(topic_exists==False):
    new_topic=NewTopic(topic=topic_to_search,num_partitions=3,replication_factor=3)

    future=admin_cl.create_topics([new_topic])

    for i in future.keys():
        try:
            future[i].result()  # The result itself is None, just checking if the operation was successful
            print(f'Topic {i} created')
        except Exception as e:
            print(f'Failed to create topic {i}: {e}')

# describe the new topic

c1=ConfigResource(restype=ConfigResource.Type.TOPIC,name=topic_to_search)

result=admin_cl.describe_configs([c1])

for i in result.keys():
    try:
        config_entries = result[i].result()
        print(f'Configuration for {i.name}:')
        print(config_entries)
    except Exception as e:
        print(f'Failed to describe configuration for {i.name}: {e}')

# alter config

config_dict = {'max.message.bytes': str(12*1024)}

c1=ConfigResource(restype=ConfigResource.Type.TOPIC,name=topic_to_search,set_config=config_dict)

result=admin_cl.alter_configs([c1])

for i in result.keys():
    try:
        result[i].result()
        print(f'Configuration for {i.name} altered successfully')
    except Exception as e:
        print(f'Failed to alter configuration for {i.name}: {e}')