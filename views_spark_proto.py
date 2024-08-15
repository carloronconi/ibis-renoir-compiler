import time
import multiprocessing as mp
from benchmark.internal.producer import Prod
from benchmark.internal.consumer import Cons
from benchmark.internal.spark_connector import create_stream_query
import random
import string


def rand_string(prefix="", len=16):
    return prefix + "".join(random.choices(string.ascii_letters, k=len))

def main():
    producer_topic = rand_string("prod_topic_")
    consumer_topic = rand_string("cons_topic_")
    
    stream_query_proc = mp.Process(target=create_stream_query, args=(producer_topic, consumer_topic))
    stream_query_proc.start()
    
    producer = Prod()
    consumer = Cons()

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

