import multiprocessing as mp
import multiprocessing.connection as con
import os
import traceback
import psutil
import benchmark.discover.load_tests as discover
import test
import codegen.benchmark as bm
from . import backend_benchmark as bb
from signal import SIGKILL


SCENARIO_PATTERN = ""
RAISE_EXCEPTIONS = False


def main():
    con = mp.Pipe(duplex=True)
    _, other = con
    proc = mp.Process(target=execute_benchmark, args=(other,))
    proc.start()
    timeout = Scenario.timeout
    police_benchmark(proc, con, timeout)


def police_benchmark(proc: mp.Process, con: tuple[con.Connection, con.Connection], timeout: int):
    pipe, other = con
    while True:
        pipe.send("start")
        request, curr_scenario, curr_test, curr_backend = pipe.recv()
        if request == "done_scenario":
            continue
        if request == "done_all":
            return
        pipe.send("permission_granted")
        
        success, exception = False, "timeout"
        if pipe.poll(timeout):
            success, exception = pipe.recv()
        if not success:
            # we kill the process both in case of exception and timeout
            # we also do that in case of exception because when flink fails, exceptions are handled 
            # badly and the JVM overflows https://github.com/py4j/py4j/issues/325
            # kill all the children of the current process so memory_profiler doesn't complain
            # we killed the process it was monitoring
            parent = psutil.Process(os.getpid())
            for child in parent.children(recursive=True):
                os.kill(child.pid, SIGKILL)
            if exception == "timeout":
                # writing to log as process handilng the log was killed
                logger = bm.Benchmark(curr_test.rsplit(".", 1)[1], Scenario.dir)
                logger.backend_name = curr_backend
                logger.scenario = curr_scenario
                logger.run_count = 0
                logger.exception = "timeout"
                logger.log()
                trace = ""
            else:
                trace = exception
                exception = "exception"
                if RAISE_EXCEPTIONS:
                    msg = trace.replace("NEWLINE_ESCAPE", "\n").replace("COMMA_ESCAPE", ",")
                    raise Exception(f"Captured exception from worker:\n{msg}")
            print(f"{exception}: {curr_test} with {curr_backend} in {curr_scenario} - trace: {trace[-50:]}")
            # restart the process from same scenario, skipping to the next backend
            proc = mp.Process(target=execute_benchmark, args=(other, curr_scenario, curr_test, curr_backend))
            proc.start()
        else:
            message = f"success: {curr_test} with {curr_backend} in {curr_scenario}"
            print(message)
            

def execute_benchmark(pipe: con.Connection, failed_scenario: str = None, failed_test: str = None, failed_backend: str = None):
    scenarios = [s for s in Scenario.__subclasses__() if SCENARIO_PATTERN in s.__name__]
    if failed_scenario:
        # run the failed scenario with special parameters to make it skip already performed tests
        # and then run the rest of the scenarios anyway
        idx = [s.__name__ for s in scenarios].index(failed_scenario)
        scenario = scenarios[idx](pipe)
        scenario.run(failed_test, failed_backend)
        scenarios = scenarios[idx + 1:]
    for S in scenarios:
        scenario = S(pipe)
        scenario.run()
    pipe.recv()
    pipe.send(("done_all", None, None, None))


class Scenario:
    runs = 1
    warmup = 1
    path_suffix = "_10"
    dir = "scenario/banana3"
    timeout = 60 * 5 # 5 minutes

    def __init__(self, pipe: con.Connection):
        # start from the next backend of the same test
        self.pipe = pipe
        self.tests_full = [t for t in discover.main() if any(pat in t for pat in self.test_patterns)]

    def run(self, failed_test: str = None, failed_backend: str = None):
        for test_full in self.tests_full:
            # skip all tests before the failed one resuming from that
            if failed_test and failed_test != test_full:
                continue
            if failed_test and failed_test == test_full:
                failed_test = None
            for backend_name in self.backend_names:
                # skip all backends before and including the failed one
                if failed_backend and failed_backend != backend_name:
                    continue
                if failed_backend and failed_backend == backend_name:
                    failed_backend = None
                    continue
                try:
                    for i in range(self.warmup + self.runs):
                        self.pipe.recv()
                        run_id = i - self.warmup if i >= self.warmup else -1
                        self.pipe.send(("permission_to_run", self.__class__.__name__, test_full, backend_name))
                        self.pipe.recv()
                        
                        test_class, test_case = test_full.rsplit(".", 1)
                        self.test_instance: test.TestCompiler = eval(f"{test_class}(\"{test_case}\")")
                        self.test_instance.benchmark = bm.Benchmark(test_case, self.dir)
                        self.test_instance.benchmark.run_count = run_id
                        test_method = getattr(self.test_instance, test_case)

                        backend = bb.BackendBenchmark.by_name(backend_name, self.test_instance, test_method)
                        self.perform_setup(backend)
                        time, memo = self.perform_measure(backend)

                        self.test_instance.benchmark.total_time_s = time
                        self.test_instance.benchmark.max_memory_MiB = memo
                        self.test_instance.benchmark.log()
                        
                        self.pipe.send((True, None))
                except Exception as e:
                    trace = " ".join(traceback.format_exception(e)).replace(",", "COMMA_ESCAPE").replace("\n", "NEWLINE_ESCAPE")
                    self.test_instance.benchmark.exception = trace
                    self.test_instance.benchmark.log()
                    self.pipe.send((False, trace))
                    # because of the issues with the JVM when using flink, the process and its children
                    # will be killed so we can return here instead of continuing with the next backend
                    return
        self.pipe.recv()
        self.pipe.send(("done_scenario", None, None, None))

    def perform_setup(self, backend: bb.BackendBenchmark):
        backend.logger.scenario = self.__class__.__name__
        self.test_instance.init_files(file_suffix=self.path_suffix)
        self.test_instance.init_tables()

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        # returns time and memory
        raise NotImplementedError


class Scenario1(Scenario):
    # Preprocessing
    # Unstructured data from external environment (e.g. MQTT, sensors) represented by files
    # is cleaned and restructured, before being stored back in files
    # - table_origin: read from file
    # - data_destination: write to file
    def __init__(self, pipe):
        self.test_patterns = ["test_scenarios_preprocess", "test_nexmark"]
        self.backend_names = ["duckdb", "flink", "renoir"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        super().perform_setup(backend)
        # only default setup is required, as loading from file means we don't need to 
        # preload the tables into the backend

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        # special to_file measure is used
        return backend.perform_measure_to_file()
    

class Scenario3(Scenario):
    # Analytics
    # Interactive data exploration, performing successive queries on the same data, simulated
    # by preloading the data into the backend and performing a first un-timed query that
    # is stored in the backend, and then performing a second timed query over that
    # - table_origin: preload table and perform computationally intensive query
    # - data_destination: none
    def __init__(self, pipe):
        self.test_patterns = ["test_scenarios_analytics", "test_nexmark"]
        self.backend_names = ["duckdb", "polars", "risingwave", "renoir"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        super().perform_setup(backend)
        backend.preload_cached_query()

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        return backend.perform_measure_cached_to_none()
    

class Scenario4(Scenario):
    # Exploration
    # Direct data exploration, performing one-shot queries directly on the data, 
    # without having it pre-loaded into a structured format
    # - table_origin: read from file
    # - data_destination: none
    def __init__(self, pipe):
        self.test_patterns = ["test_scenarios_exploration", "test_nexmark"]
        self.backend_names = ["duckdb", "polars", "flink", "renoir"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        super().perform_setup(backend)

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        return backend.perform_measure_to_none()


if __name__ == "__main__":
    main()
