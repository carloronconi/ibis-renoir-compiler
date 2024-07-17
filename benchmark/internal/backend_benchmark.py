class BackendBenchmark():
    @classmethod
    def by_name(cls, name: str) -> "BackendBenchmark":
        match name:
            case "duckdb": 
                return DuckdbBenchmark()

    def __init__(self) -> None:
        pass
    def preload_tables():
        pass
    def perform_initial_query():
        pass
    def perform_query():
        pass
    def store_result():
        pass

class DuckdbBenchmark(BackendBenchmark):
    def __init__(self) -> None:
        pass
    def preload_tables():
        pass
    def perform_initial_query():
        pass
    def perform_query():
        pass
    def store_result():
        pass