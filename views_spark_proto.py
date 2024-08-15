import time
import multiprocessing as mp
from benchmark.internal.producer import Prod
from benchmark.internal.consumer import Cons
from benchmark.internal.spark_connector import create_stream_query
import random
import string
# import confluent_kafka.admin, pprint


def rand_string(prefix="", len=16):
    return prefix + "".join(random.choices(string.ascii_letters, k=len))


# this breaks it! this way even when consuming the producer topic directly, kafka is stuck
# fixed only by restarting kafka
# def init_topics(new_topics):
#     admin = confluent_kafka.admin.AdminClient({'bootstrap.servers': 'localhost:9092'})
#     futs = admin.delete_topics(list(admin.list_topics().topics.keys()))
#     for topic, future in futs.items():
#         try:
#             future.result()
#             print(f"Deleted topic {topic}")
#         except Exception as e:
#             print(f"Failed to delete topic {topic}: {e}")
# 
#     futs = admin.create_topics([confluent_kafka.admin.NewTopic(topic, num_partitions=1, replication_factor=1) 
#                          for topic in new_topics])
#     for topic, future in futs.items():
#         try:
#             future.result()
#             print(f"Created topic {topic}")
#         except Exception as e:
#             print(f"Failed to create topic {topic}: {e}")
#             
#     print(f"New topics:\n{new_topics}")
#     print(f"Current topics:\n{pprint.pformat(admin.list_topics().topics)}")


def main():
    producer_topic = rand_string("prod_topic_")
    consumer_topic = rand_string("cons_topic_")
    
    stream_query_proc = mp.Process(target=create_stream_query, args=(producer_topic, consumer_topic))
    stream_query_proc.start()
    
    producer = Prod()
    consumer = Cons()

    # consumer can't subscribe to non-existing topic, so produce single message to create it
    # and discard it from consumer
    producer.produce(consumer_topic, amount=1, no_cb=True)
    result = consumer.consume(consumer_topic, max_messages=1)
    print(f"Created consumer topic and read message {result}")

    start_time = time.perf_counter()
    producer.produce(producer_topic)
    result = consumer.consume(consumer_topic)
    end_time = time.perf_counter()

    if result:
        print(f"Successfully consumed {result} messages in {end_time - start_time} seconds")  
    else:
        print("Failed to consume any messages")  
    
    # TODO: stream_query_proc.kill()

if __name__ == "__main__":
    main()

