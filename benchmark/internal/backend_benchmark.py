import time

import pandas as pd
import test
import ibis
from pyflink.table import EnvironmentSettings, TableEnvironment
from memory_profiler import memory_usage
from codegen import compile_preloaded_tables_evcxr
from ibis import _
from . import internal_benchmark as ib


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
    
    def perform_measure_cached_to_none(self) -> tuple[float, float]:
        # by default, same behavior as perform_measure_to_none
        return self.perform_measure_to_none()
    
    def perform_measure_to_none(self) -> tuple[float, float]:
        def run(test_method, test_instance):
            test_method()
            test_instance.query.execute()
        start_time = time.perf_counter()
        memo = memory_usage((run, [self.test_method, self.test_instance]), include_children=True)
        end_time = time.perf_counter()
        return end_time - start_time, max(memo)

    def perform_measure_to_file(self)-> tuple[float, float]:
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
    
    def preload_cached_query(self):
        # by default we preload tables using create_table
        con = ibis.get_backend()
        for name, table in self.test_instance.tables.items():
            if name == "ints_strings":
                modified_table = (table
                                  .group_by(_.int1)
                                  .aggregate(int4=_.int4.sum(), string1=_.string1.first())
                                  .execute())
            else:
                modified_table = table.execute()
            self.test_instance.tables[name] = con.create_table(
                name, modified_table, overwrite=True)
            
    def preload_cached_query_without_csv(self):
        # These backends don't allow reading from csv so self.tables is empty and we create it from scratch here.
        # Because the create table for these backends is extremely slow, we first check if the tables are
        # already in place and of the right size: if so, we skip the creation.
        cache_con = ibis.duckdb.connect()
        con = ibis.get_backend()
        tables = {}
        for name, file_path in self.test_instance.files.items():
            if name == "ints_strings":
                modified_table = (cache_con
                                  .read_csv(file_path)
                                  .group_by(_.int1)
                                  .aggregate(agg=_.int4.sum(), string1=_.string1.first())
                                  .to_pandas())
            else:
                modified_table = pd.read_csv(file_path)
            if name in con.list_tables() and con.table(name).count().execute() == modified_table.shape[0]:
                tables[name] = con.table(name)
                continue
            print(f"Creating table {name} in {con.name} from {file_path}. Could take a while: might need to increase timeout...")
            tables[name] = con.create_table(name, modified_table, overwrite=True)
        self.test_instance.tables = tables
    

class RenoirBenchmark(BackendBenchmark):
    name = "renoir"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend("duckdb://")
        self.test_instance.perform_compilation = True

    def perform_measure_compile_and_run(self) -> tuple[float, float]:
        start_time = time.perf_counter()
        memo = memory_usage((self.test_method,), include_children=True)
        end_time = time.perf_counter()
        return end_time - start_time, max(memo)

    def perform_measure_to_file(self) -> tuple[float, float]:
        # override to run with renoir instead of ibis
        self.test_instance.print_output_to_file = True
        return self.perform_measure_compile_and_run()
    
    def perform_measure_to_none(self) -> tuple[float, float]:
        # for renoir, we don't need to call query.execute() as the perform_compilation
        # flag is already set
        # test_instance also has a renoir_cached flag which was interfering when the 
        # scenario with .compile_preloaded_tables_evcxr was called, but fixed inside that func
        self.test_instance.print_output_to_file = False
        return self.perform_measure_compile_and_run()

    def perform_measure_cached_to_none(self) -> tuple[float, float]:
        self.test_instance.renoir_cached = True
        memo, total_time = ib.run_async_from_sync(self.test_instance.run_evcxr(self.test_method))
        self.test_instance.renoir_cached = False
        return total_time, memo

    def preload_cached_query(self):
        # override to use evcxr, which for now has hardcoded cached query
        files = self.test_instance.files
        tables = self.test_instance.tables
        compile_preloaded_tables_evcxr([(files[k], tables[k]) for k in files.keys()])

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
        
    def preload_cached_query(self):
        return super().preload_cached_query_without_csv()


class RisingwaveBenchmark(BackendBenchmark):
    name = "risingwave"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        ibis.set_backend(ibis.risingwave.connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev",))
        
    def preload_cached_query(self):
        return super().preload_cached_query_without_csv()
