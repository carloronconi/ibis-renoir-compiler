import multiprocessing as mp
import multiprocessing.connection as con
import benchmark.discover.load_tests as discover
import test
from . import internal_benchmark as ib
from . import backend_benchmark as bb


def main():
    con = mp.Pipe(duplex=True)
    _, other = con
    proc = mp.Process(target=execute_benchmark, args=(other,))
    proc.start()
    count = Scenario.warmup + Scenario.runs
    timeout = Scenario.timeout
    police_benchmark(proc, con, timeout)


def police_benchmark(proc: mp.Process, con: tuple[con.Connection, con.Connection], timeout: int):
    pipe, other = con
    while True:
        request, curr_scenario, curr_test, curr_backend = pipe.recv()
        if request == "done":
            return
        pipe.send("permission_granted")
        if not pipe.poll(timeout):
            proc.kill()
            proc.join()
            print("timeout: killed process")
            # restart the process from same scenario, skipping to the next backend
            proc = mp.Process(target=execute_benchmark, args=(other, curr_scenario, curr_test, curr_backend))
            proc.start()
            continue
        success, _ = pipe.recv()
        print(success)
        # same behavior for success or failure: the process itself will skip to next backend
            

def execute_benchmark(pipe: con.Connection, failed_scenario: str = None, failed_test: str = None, failed_backend: str = None):
    scenarios = Scenario.all()
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
    timeout = 10 # 5 minutes

    def __init__(self, pipe: con.Connection):
        # start from the next backend of the same test
        self.pipe = pipe
        self.tests = self.discover_tests()
        self.backends = [(b, bb.BackendBenchmark.by_name(b)) for b in self.backend_names]

    def discover_tests(self) -> list[tuple[str, str, str]]:
        tests_full = [t for t in discover.main() if any(pat in t for pat in self.test_patterns)]
        return [t.rsplit(".", 1) + [t] for t in tests_full]
        
    def run(self, failed_test: str = None, failed_backend: str = None):
        for test_class, test_case, test_full in self.tests:
            # skip all tests before the failed one resuming from that
            if failed_test and failed_test != test_full:
                continue
            if failed_test and failed_test == test_full:
                failed_test = None
            test_instance: test.TestCompiler = eval(f"{test_class}(\"{test_case}\")")
            for backend_name, backend in self.backends:
                # skip all backends before and including the failed one
                if failed_backend and failed_backend != backend_name:
                    continue
                if failed_backend and failed_backend == backend_name:
                    failed_backend = None
                    continue
                try:
                    for i in range(self.warmup + self.runs):
                        run_id = i - self.warmup if i >= self.warmup else -1
                        self.pipe.send(("permission_to_run", self.__class__.__name__, test_full, backend_name))
                        self.pipe.recv()
                        
                        self.perform_setup(backend)
                        time, memo = self.perform_measure(backend)
                        
                        self.pipe.send((True, backend_name, test_case, run_id, time, memo))
                except:
                    self.pipe.send((False, backend_name))
        self.pipe.send(("done", None))

    def perform_setup(self, backend: bb.BackendBenchmark):
        raise NotImplementedError

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        # returns time and memory
        raise NotImplementedError

    @classmethod
    def all(cls):
        subclasses = []
        stack = [cls]
        while stack:
            curr = stack.pop()
            subclasses: list[type[Scenario]] = curr.__subclasses__()
            if subclasses:
                stack.extend(subclasses)
            else:
                subclasses.append(curr)
        return subclasses


class Scenario1(Scenario):
    # Preprocessing
    def __init__(self, pipe):
        self.table_origin = "file"
        self.data_destination = "file"
        self.test_patterns = ["test_nexmark"]
        self.backend_names = ["duckdb", "flink"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        print(f"scenario 1 - {backend.__class__.__name__} sleeping...")
        while(True):  
            pass  

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        raise NotImplementedError


class Scenario2(Scenario):
    # Test
    def __init__(self, pipe):
        self.table_origin = "file"
        self.data_destination = "file"
        self.test_patterns = ["test_nullable"]
        self.backend_names = ["duckdb", "flink"]
        super().__init__(pipe)

    def perform_setup(self, backend: bb.BackendBenchmark):
        print("setup")

    def perform_measure(self, backend: bb.BackendBenchmark) -> tuple[float, float]:
        print("measure")
    

if __name__ == "__main__":
    main()
