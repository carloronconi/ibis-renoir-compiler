import time
import multiprocessing as mp
from benchmark.internal.views_scenario.producer import Prod
from benchmark.internal.views_scenario.consumer import Cons
from benchmark.internal.views_scenario.stream_query import create_stream_query
import random
import string
import sys


def rand_string(prefix="", len=16):
    return prefix.lower() + "".join(random.choices(string.ascii_lowercase, k=len))


def main():
    backend = sys.argv[1]

    producer_topic = rand_string("prod_topic_")
    consumer_topic = rand_string("cons_topic_")
    
    producer = Prod()
    consumer = Cons()

    # consumer can't subscribe to non-existing topic, so produce single message to create it
    # and discard it from consumer
    producer.produce(consumer_topic, amount=1)
    result = consumer.consume(consumer_topic, max_messages=1, do_close=False)
    print(f"Created consumer topic and consumed message {result}")
    # flink connector still works even if defined before the topic producer topic is created, but risingwave doesn't
    # so better be sure and put additional message in the producer topic
    producer.produce(producer_topic, amount=1)
    print("Created producer topic without consuming messages")

    stream_query_proc = mp.Process(target=create_stream_query, 
                                   args=(backend, producer_topic, consumer_topic, "views_1_filter"))
    stream_query_proc.start()

    start_time = time.perf_counter()
    producer.produce(producer_topic)
    result = consumer.consume(consumer_topic)
    end_time = time.perf_counter()

    if result:
        print(f"Successfully consumed {result} messages in {end_time - start_time} seconds")  
    else:
        print("Failed to consume any messages")  
    
    stream_query_proc.kill()

if __name__ == "__main__":
    main()

