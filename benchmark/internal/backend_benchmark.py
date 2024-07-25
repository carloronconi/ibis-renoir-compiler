import subprocess
import time

import pandas as pd
import test
import ibis
from memory_profiler import memory_usage
try:
    from codegen import compile_preloaded_tables_evcxr
except ImportError:
    print("Skipped import of ibis-renoir-compiler because of wrong version of ibis: it only supports ibis 8.0.0")
from ibis import _
from . import internal_benchmark as ib
from .kafka_io import Producer, Consumer
try:
    from pyflink.java_gateway import get_gateway
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
except ModuleNotFoundError:
    print("Skipped import of pyflink because of missing dependencies")
from pyspark.sql import SparkSession
import ibis.backends
import ibis.backends.pyspark
from pyspark.sql import SparkSession
from ibis.backends.risingwave import Backend as RisingwaveBackend
from pyspark.sql.streaming import StreamingQuery
from threading import Thread


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
                                  .group_by(_.string1)
                                  .aggregate(int4=_.int4.sum(), int1=_.int1.max())
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
                                  .group_by(_.string1)
                                  .aggregate(int4=_.int4.sum(), int1=_.int1.max())
                                  .to_pandas())
            else:
                modified_table = pd.read_csv(file_path)
            if name in con.list_tables() and con.table(name).count().execute() == modified_table.shape[0]:
                tables[name] = con.table(name)
                continue
            print(f"Creating table {name} in {con.name} from {file_path}. Could take a while: might need to increase timeout...")
            tables[name] = con.create_table(name, modified_table, overwrite=True)
        self.test_instance.tables = tables

    def perform_measure_to_kafka(self) -> tuple[float, float]:
        self.test_method()
        self.create_view()
        # create sink needs to run in separate thread for pyflink otherwise it blocks
        self.subp = Thread(target=self.create_sink)
        self.subp.start()
        return self.perform_measure_latency_kafka_to_kafka()
    
    def perform_measure_latency_kafka_to_kafka(self) -> tuple[float, float]:
        print("\n\n\narrived in timer\n\n\n")
        producer = Producer()
        consumer = Consumer()
        start_time = time.perf_counter()
        # the backend is already set up to update its internal view and
        # write it to the sink topic
        producer.write_datum()
        consumer.read_datum()
        end_time = time.perf_counter()
        print("\n\n\nfinished in timer\n\n\n")
        self.do_stop = True
        self.subp.join()
        # TODO: how measure memory of external risingwave/kafka within docker?
        return end_time - start_time, None
    

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
        return total_time, max(memo)

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
        # connecting to a standalone flink cluster instead of the built-in one
        # re-starting flink instance because in case of failures it doesn't recover
        subprocess.run("./benchmark/stop_flink.sh > /dev/null", shell=True)
        subprocess.run("./benchmark/start_flink.sh > /dev/null", shell=True)
        gateway = get_gateway()
        string_class = gateway.jvm.java.lang.String
        string_array = gateway.new_array(string_class, 0)
        stream_env = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        j_stream_execution_environment = stream_env.createRemoteEnvironment(
            "localhost", 
            8081, 
            string_array)

        table_env = StreamTableEnvironment.create(
            StreamExecutionEnvironment(j_stream_execution_environment),
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
    
    def create_view(self):
        con: RisingwaveBackend = ibis.get_backend()
        con.create_materialized_view("view_kafka", 
                             obj=self.test_instance.query, 
                             overwrite=True)   

    def create_sink(self):
        con: RisingwaveBackend = ibis.get_backend()
        con.create_sink("sink_kafka",
                sink_from="view_kafka",
                connector_properties={"connector": "kafka",
                                      "topic": "sink",
                                      "properties.bootstrap.server": "localhost:9092"},
                data_format="PLAIN",
                encode_format="JSON")     


class SparkBenchmark(BackendBenchmark):
    name = "spark"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        # before running this, cd to ./benchmark/compose-kafka and do `docker-compose up`
        # if there are any errors in the Dockerfile an you need to rebuild, clean the everything
        # first with `docker system prune` (it prunes everything unused so careful on server) and 
        # then `docker-compose build --no-cache` and again `docker-compose up`
        scala_version = '2.12'
        spark_version = '3.1.2'
        # ensure match above values match the correct versions in pip
        packages = [
            f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
            'org.apache.kafka:kafka-clients:3.2.1'
        ]
        session = SparkSession.builder\
           .master("local[*]")\
           .config("spark.executor.memory", "16g")\
           .config("spark.driver.memory", "16g")\
           .appName("kafka-example")\
           .config("spark.jars.packages", ",".join(packages))\
           .getOrCreate()

        con: ibis.backends.pyspark.Backend = ibis.pyspark.connect(session, mode="streaming")
        ibis.set_backend(con)
        self.do_stop = False

    def create_view(self):
        con = ibis.get_backend()
        self.view = con.create_view("view_kafka", self.test_instance.query, overwrite=True)
    
    def create_sink(self):
        con: ibis.backends.pyspark.Backend = ibis.get_backend()
        # notes:
        # - create a checkpointLocation on the host of this script (not the kafka container!)
        # - call .start() on .to_kafka(), docs are wrong and that actually returns a DataStreamWriter, 
        #   to get a StreamingQuery you need to call .start()
        stream_query: StreamingQuery = con.to_kafka(self.view, options={"kafka.bootstrap.servers": "localhost:9092", 
                            "topic": "sink",
                            "checkpointLocation": "/tmp/spark_checkpoint"}).start()
        # the query doesn't run in the background! If we don't await it here, pyspark will exit immediately
        while stream_query.isActive and not self.do_stop:
            stream_query.awaitTermination(0.1)
        stream_query.stop()

