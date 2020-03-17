from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException, KafkaError, libversion

def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
#topic in set(t.topic for t in iter(topic_metadata.topics.values()))

    #Produce request error below comment out line
    #@client = AdminClient({"bootstrap.servers": "PLAINTEXT://kafka:9092"})
#    client = AdminClient({"bootstrap.servers": "localhost:9092"})
#    topic_metadata = client.list_topics(timeout=5)
#    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

#def topic_exists():
#    """Checks if the given topic exists in Kafka"""
#    print('checking')
    #try :
        #client = AdminClient({"bootstrap.servers": "PLAINTEXT://kafka0:9092"})
    #    client = AdminClient({"bootstrap.servers": "PLAINTEXT://kafka0:9092"})

    #except:
    #    print ('cant connect to k0')

    #Produce request error below comment out line
    #@client = AdminClient({"bootstrap.servers": "PLAINTEXT://kafka:9092"})
#    client = AdminClient({"bootstrap.servers": "localhost:9092"})
    #topic_metadata = client.list_topics(timeout=5)
    #return topic in set(t.topic for t in iter(topic_metadata.topics.values()))
