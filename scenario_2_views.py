import time
import multiprocessing as mp
import random
import string
from benchmark.internal.views_scenario.producer import Prod
from benchmark.internal.views_scenario.consumer import Cons
from benchmark.internal.views_scenario.stream_query import create_stream_query
from benchmark.internal.views_scenario.test import TestViewsCustom, TestViewsNexmark
from codegen import Benchmark as Logger


def rand_string(prefix="", len=16):
        return prefix.lower() + "".join(random.choices(string.ascii_lowercase, k=len))


def produce(TestClass, stream_name, topic: str, amount: int):
    print(f"Started worker producer process for {stream_name} to {topic}")
    generator = next(s for s in TestClass.streams if s.name == stream_name).generator()
    producer = Prod(generator)
    producer.produce(topic, amount=amount)


class ViewsScenario:
    def run_once(self, backend: str, TestClass, test_query, run_count: int):
        producer_topics = {rand_string(f"prod_topic_{s.name}_"): s for s in TestClass.streams}
        consumer_topic = rand_string("cons_topic_")
        
        consumer = Cons()
        def p():
            while True:
                yield {"INIT": "MESSAGE"}
        help_producer = Prod(p())

        # create consumer topic and consume message
        help_producer.produce(consumer_topic, amount=1)
        result, _, _ = consumer.consume(consumer_topic, max_messages=1, do_close=False)
        print(f"Created consumer topic and consumed message {result}")
        # create all producer topics without consuming messages 
        # (can't have more consumers for same topic partition)
        for topic in producer_topics.keys():
            help_producer.produce(topic, amount=1)
        print("Created producer topics without consuming messages")

        recv, send = mp.Pipe(duplex=False)
        stream_query_proc = mp.Process(target=create_stream_query, 
                                       args=(backend, 
                                             {t: stream.schema for t, stream in producer_topics.items()}, 
                                             consumer_topic, 
                                             test_query,
                                             send))
        stream_query_proc.start()
        producer_pool = mp.Pool(len(producer_topics))

        start_time = time.perf_counter()
        result = producer_pool.starmap_async(produce, [(TestClass, 
                                         stream.name, 
                                         topic, 
                                         self.dataset_size) 
                                         for topic, stream in producer_topics.items()])
        result.wait()
        result, end_time, exception = consumer.consume(consumer_topic, 
                                                       stream_pipe=recv)

        if result:
            print(f"Successfully consumed {len(result)} messages in {end_time - start_time} seconds. Messages:\n{result}")
        else:
            end_time = -1
            if not exception:
                # no result and no exception
                exception = "no_messages_consumed"
            fmt = exception.replace("NEWLINE_ESCAPE", "\n").replace("COMMA_ESCAPE", ",")[-50:]
            print(f"Failed to consume any messages with exception:\n{fmt}")

        self.logger.test_name = test_query.__name__
        self.logger.backend_name = backend
        self.logger.run_count = run_count
        self.logger.total_time_s = end_time - start_time
        self.logger.scenario = "Scenario2"
        self.logger.exception = exception
        self.logger.log()

        if not result and self.raise_exceptions:
            msg = exception.replace("NEWLINE_ESCAPE", "\n").replace("COMMA_ESCAPE", ",")
            raise Exception(f"Captured exception from worker:\n{msg}")

        stream_query_proc.kill()

        if not result: return False
        return True


    def main(self):
        backends = ["spark", "risingwave"]
        test_classes = [TestViewsNexmark]
        test_pattern = "4"
        runs = 5
        warmup = 1
        self.dataset_size = 10000000
        dir = "scenario/banana"
        self.raise_exceptions = False

        self.logger = Logger("", dir)
        for TestClass in test_classes:
            queries = [method for name, method in TestClass.__dict__.items() if "test_" in name and test_pattern in name]
            for query in queries:
                for backend in backends:
                    print(f"Running {query.__name__} on {backend}")
                    for _ in range(warmup):
                        success = self.run_once(backend, TestClass, query, -1)
                        if not success:
                            break
                    if not success:
                        continue
                    for i in range(runs):
                        if not self.run_once(backend, TestClass, query, i):
                            break


if __name__ == "__main__":
    scenario = ViewsScenario()
    scenario.main()

