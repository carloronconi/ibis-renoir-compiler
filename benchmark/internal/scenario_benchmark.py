import multiprocessing as mp
import multiprocessing.connection as con
import benchmark.discover.load_tests as discover
from . import internal_benchmark as ib
from . import backend_benchmark as bb


def main():
    main, worker = mp.Pipe(duplex=True)
    proc = mp.Process(target=execute_benchmark, args=(worker))
    proc.start()
    count = Scenario.warmup + Scenario.runs
    timeout = Scenario.timeout
    police_benchmark(proc, main, timeout)


def police_benchmark(proc: mp.Process, con: tuple[con.Connection, con.Connection], timeout: int):
    pipe, other = con
    while True:
        request, current_backend = pipe.recv()
        if request == "done":
            return
        pipe.send("permission_granted")
        if not pipe.poll(timeout):
            proc.kill()
            proc.join()
            # restart the process skipping the successful backend runs plus the next one
            proc = mp.Process(target=execute_benchmark, args=(other, current_backend))
            continue
        success, _ = pipe.recv()
        # same behavior for success or failure: the process itself will skip to next backend
            

def execute_benchmark(pipe: con.Connection):
    scenarios = Scenario.all(pipe)
    for scenario in scenarios:
        scenario.run()


class Scenario:
    runs = 5
    warmup = 1
    path_suffix = ""
    dir = "banana"
    timeout = 60 * 5 # 5 minutes

    def __init__(self, pipe: con.Connection, killed_backend: str = None):
        # if killed_backend is provided, start from the next backend
        self.pipe = pipe
        self.tests = self.discover_tests()
        self.backends = [(b, bb.BackendBenchmark.by_name(b)) for b in self.backend_names]
        if killed_backend:
            self.backends = self.backends[self.backend_names.index(killed_backend) + 1:]

    def discover_tests(self):
        tests_full = [t for t in discover.main() if any(pat in t for pat in self.test_patterns)]
        self.tests: list[tuple[str, str]] = [t.rsplit(".", 1) for t in tests_full]
        
    def run(self):
        for test_class, test_case in self.tests:
            for backend_name, backend in self.backends:
                try:
                    for i in range(self.warmup + self.runs):
                        run_id = i - self.warmup if i >= self.warmup else -1
                        self.pipe.send(("permission_to_run", backend_name))
                        self.pipe.recv()
                        
                        self.perform_setup()
                        time, memo = self.perform_measure()
                        
                        self.pipe.send((True, backend_name, test_case, run_id, time, memo))
                except:
                    self.pipe.send((False, backend_name))
        self.pipe.send(("done", None))

    def perform_setup(self):
        raise NotImplementedError

    def perform_measure(self) -> tuple[float, float]:
        # returns time and memory
        raise NotImplementedError

    @classmethod
    def all(cls, pipe):
        subclasses = []
        stack = [cls]
        while stack:
            curr = stack.pop()
            subclasses: list[type[Scenario]] = sorted(curr.__subclasses__())
            if subclasses:
                stack.extend(subclasses)
            else:
                subclasses.append(curr)

        scenarios = [S(pipe) for S in subclasses]
        return scenarios


class Scenario1(Scenario):
    # Preprocessing
    def __init__(self, pipe):
        self.table_origin = "file"
        self.data_destination = "file"
        self.test_patterns = ["test_nullable"]
        self.backend_names = ["duckdb", "flink", "postgres", "risingwave", "renoir"]
        super().__init__(pipe)


if __name__ == "__main__":
    main()
