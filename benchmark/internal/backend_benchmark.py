import test


class BackendBenchmark():
    @classmethod
    def by_name(cls, name: str, test_full: str) -> "BackendBenchmark":
        match name:
            case "duckdb": 
                return DuckdbBenchmark(test_full)
            case "flink":
                return FlinkBenchmark(test_full)
            case _:
                raise ValueError(f"Unknown backend {name}")

    def __init__(self, test_full) -> None:
        test_class, test_case = test_full.rsplit(".", 1)
        self.test_instance: test.TestCompiler = eval(f"{test_class}(\"{test_case}\")")

    def preload_tables():
        pass
    def perform_initial_query():
        pass
    def perform_query():
        pass
    def store_result():
        pass

class DuckdbBenchmark(BackendBenchmark):
    def __init__(self, test_full) -> None:
        super().__init__(test_full)

    def preload_tables():
        pass
    def perform_initial_query():
        pass
    def perform_query():
        pass
    def store_result():
        pass

class FlinkBenchmark(BackendBenchmark):
    def __init__(self, test_full) -> None:
        super().__init__(test_full)

    def preload_tables():
        pass
    def perform_initial_query():
        pass
    def perform_query():
        pass
    def store_result():
        pass