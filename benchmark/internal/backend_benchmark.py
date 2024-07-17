import time
import test
import ibis
from pyflink.table import EnvironmentSettings, TableEnvironment
from memory_profiler import memory_usage



class BackendBenchmark():
    @classmethod
    def by_name(cls, name: str, test_instance: test.TestCompiler, test_method) -> "BackendBenchmark":
        subclasses = cls.__subclasses__()
        for Sub in subclasses:
            if Sub.name == name:
                return Sub(test_instance, test_method)
        raise ValueError(f"No subclass with name {name}")

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        test_instance.benchmark.backend_name = self.name
        test_instance.init_benchmark_settings()
        
        self.test_instance = test_instance
        self.test_method = test_method

    @property
    def logger(self):
        return self.test_instance.benchmark

    def perform_query_to_file(self)-> tuple[float, float]:
        def run(test_method, test_instance):
            test_method()
            result = test_instance.query.execute()
            # create a string with the result and store it
            result = result.to_csv()
            with open("./out/ibis-backend-result.csv", "w") as file:
                file.write(result)
        start_time = time.perf_counter()
        memo = memory_usage((run, [self.test_method, self.test_instance]), include_children=True)
        end_time = time.perf_counter()
        return end_time - start_time, max(memo)


class RenoirBenchmark(BackendBenchmark):
    name = "renoir"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend("duckdb://")
        self.test_instance.perform_compilation = True

    def perform_query_to_file(self):
        # overrides to run with renoir instead of ibis
        self.test_instance.print_output_to_file = True
        start_time = time.perf_counter()
        memo = memory_usage((self.test_method,), include_children=True)
        end_time = time.perf_counter()
        return end_time - start_time, max(memo)

class DuckdbBenchmark(BackendBenchmark):
    name = "duckdb"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend("duckdb://")
        

class FlinkBenchmark(BackendBenchmark):
    name = "flink"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        table_env = TableEnvironment.create(
                EnvironmentSettings.in_streaming_mode())
        con = ibis.flink.connect(table_env)
        ibis.set_backend(con)


class PolarsBenchmark(BackendBenchmark):
    name = "polars"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend(self.name)


class PostgresBenchmark(BackendBenchmark):
    name = "postgres"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend(ibis.postgres.connect(
                user="postgres",
                password="postgres",
                host="localhost",
                port=5432,
                database="postgres"))


class RisingwaveBenchmark(BackendBenchmark):
    name = "risingwave"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend(ibis.risingwave.connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev",))
