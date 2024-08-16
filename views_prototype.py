import time
import multiprocessing as mp
import random
import string
from benchmark.internal.views_scenario.producer import Prod
from benchmark.internal.views_scenario.consumer import Cons
from benchmark.internal.views_scenario.stream_query import create_stream_query
from benchmark.internal.views_scenario.test import TestViews
from codegen import Benchmark as Logger


def rand_string(prefix="", len=16):
    return prefix.lower() + "".join(random.choices(string.ascii_lowercase, k=len))


def run_once(backend: str, test_name: str, run_count: int, logger: Logger, dataset_size: int):
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
                                   args=(backend, producer_topic, consumer_topic, test_name))
    stream_query_proc.start()

    start_time = time.perf_counter()
    producer.produce(producer_topic, amount=dataset_size)
    result = consumer.consume(consumer_topic)
    end_time = time.perf_counter()

    if result:
        print(f"Successfully consumed {result} messages in {end_time - start_time} seconds")
        exception = None
    else:
        print("Failed to consume any messages")
        exception = "no_messages"  

    logger.test_name = test_name
    logger.backend_name = backend
    logger.run_count = run_count
    logger.total_time_s = end_time - start_time
    logger.scenario = "Scenario2"
    logger.exception = exception
    logger.log()
    
    stream_query_proc.kill()


def main():
    backends = ["spark", "risingwave"]
    test_pattern = "test_scenarios_views"
    runs = 5
    warmup = 1
    dataset_size = 100
    dir = "scenario/banana_100"

    logger = Logger("", dir)
    test_names = [name for name, _ in TestViews.__dict__.items() if test_pattern in name]
    for test_name in test_names:
        for backend in backends:
            print(f"Running {test_name} on {backend}")
            for _ in range(warmup):
                run_once(backend, test_name, -1, logger, dataset_size)
            for i in range(runs):
                run_once(backend, test_name, i, logger, dataset_size)


if __name__ == "__main__":
    main()

