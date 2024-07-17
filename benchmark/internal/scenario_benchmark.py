from . import internal_benchmark as ib
from . import backend_benchmark as bb


def main():
    for scenario in Scenario.all():
        ib.run_benchmark_with_args(scenario)


class Scenario:

    def __init__(self):
        self.runs = 5
        self.warmup = 1
        self.path_suffix = ""
        self.dir = "banana"

    def run(self):
        raise NotImplementedError
    
    def run_timed_portion(self):
        pass

    @classmethod
    def all(cls):
        subclasses = []
        stack = [cls]
        while stack:
            curr = stack.pop()
            subclasses: list[type[Scenario]] = sorted(curr.__subclasses__())
            if subclasses:
                stack.extend(subclasses)
            else:
                subclasses.append(curr)

        scenarios = [S() for S in subclasses]
        return scenarios


class Scenario1(Scenario):
    # Preprocessing
    def __init__(self):
        self.table_origin = "file"
        self.data_destination = "file"
        self.test_patterns = ["test_nullable"]
        self.backends = ["duckdb", "flink", "postgres", "risingwave", "renoir"]
        super().__init__()


if __name__ == "__main__":
    main()
