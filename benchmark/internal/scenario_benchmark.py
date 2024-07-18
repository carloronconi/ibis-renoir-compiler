import multiprocessing as mp
import multiprocessing.connection as con
import os
import traceback
import psutil
import benchmark.discover.load_tests as discover
import test
import codegen.benchmark as bm
from . import internal_benchmark as ib
from . import backend_benchmark as bb
from signal import SIGKILL


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
        if request == "done":
            return
        pipe.send("permission_granted")
        if not pipe.poll(timeout):
            
            # kill all the children of the current process so memory_profiler doesn't complain
            # we killed the process it was monitoring
            parent = psutil.Process(os.getpid())
            for child in parent.children(recursive=True):
                os.kill(child.pid, SIGKILL)

            print("timeout: killed process")
            # restart the process from same scenario, skipping to the next backend
            proc = mp.Process(target=execute_benchmark, args=(other, curr_scenario, curr_test, curr_backend))
            proc.start()
            continue
        success, _ = pipe.recv()
        print(success)
        # same behavior for success or failure: the process itself will skip to next backend
            

def execute_benchmark(pipe: con.Connection, failed_scenario: str = None, failed_test: str = None, failed_backend: str = None):
    scenarios = Scenario.__subclasses__()
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


class Scenario:
    runs = 1
    warmup = 1
    path_suffix = ""
    dir = "banana"
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
                        self.test_instance.benchmark = bm.Benchmark(test_case, dir)
                        self.test_instance.benchmark.run_count = run_id
                        test_method = getattr(self.test_instance, test_case)

                        backend = bb.BackendBenchmark.by_name(backend_name, self.test_instance, test_method)
                        self.perform_setup(backend)
                        time, memo = self.perform_measure(backend)

                        self.test_instance.benchmark.total_time_s = time
                        self.test_instance.benchmark.max_memory_MiB = memo
                        self.test_instance.benchmark.log()
                        
                        self.pipe.send((True, ""))
                except Exception as e:
                    trace = " ".join(traceback.format_exception(e)).replace(",", "COMMA_ESCAPE").replace("\n", "NEWLINE_ESCAPE")
                    self.test_instance.benchmark.exception = trace
                    self.test_instance.benchmark.log()
                    self.pipe.send((False, trace))
        self.pipe.recv()
        self.pipe.send(("done", None, None, None))

    def perform_setup(self, backend: bb.BackendBenchmark):
        backend.logger.scenario = self.__class__.__name__
        self.test_instance.init_files(file_suffix=self.path_suffix)
        self.test_instance.init_tables()

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        # returns time and memory
        raise NotImplementedError


class Scenario1(Scenario):
    # Preprocessing
    def __init__(self, pipe):
        self.table_origin = "file"
        self.data_destination = "file"
        self.test_patterns = ["test_nexmark"]
        self.backend_names = ["duckdb", "renoir"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        super().perform_setup(backend)

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        return backend.perform_query_to_file()
    

if __name__ == "__main__":
    main()
