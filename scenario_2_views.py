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

class ViewsScenario:
    def run_once(self, backend: str, TestClass, test_query, run_count: int):
        producer_topic = rand_string("prod_topic_")
        consumer_topic = rand_string("cons_topic_")

        producer = Prod(TestClass.dict_generator())
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
                                       args=(backend, producer_topic, consumer_topic, TestClass.schema, test_query))
        stream_query_proc.start()

        start_time = time.perf_counter()
        producer.produce(producer_topic, amount=self.dataset_size)
        result = consumer.consume(consumer_topic)
        end_time = time.perf_counter()

        if result:
            print(f"Successfully consumed {result} messages in {end_time - start_time} seconds")
            exception = None
        else:
            print("Failed to consume any messages")
            exception = "no_messages"  

        self.logger.test_name = test_query.__name__
        self.logger.backend_name = backend
        self.logger.run_count = run_count
        self.logger.total_time_s = end_time - start_time
        self.logger.scenario = "Scenario2"
        self.logger.exception = exception
        self.logger.log()

        stream_query_proc.kill()


    def main(self):
        backends = ["spark", "risingwave"]
        test_classes = [TestViews]
        runs = 5
        warmup = 1
        self.dataset_size = 100
        dir = "scenario/banana_100"

        self.logger = Logger("", dir)
        for TestClass in test_classes:
            queries = [method for name, method in TestClass.__dict__.items() if "test_" in name]
            for query in queries:
                for backend in backends:
                    print(f"Running {query.__name__} on {backend}")
                    for _ in range(warmup):
                        self.run_once(backend, TestClass, query, -1)
                    for i in range(runs):
                        self.run_once(backend, TestClass, query, i)


if __name__ == "__main__":
    scenario = ViewsScenario()
    scenario.main()

