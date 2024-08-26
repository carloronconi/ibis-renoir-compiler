import subprocess
import time

import ibis.backends.flink
import ibis.backends.risingwave
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
try:
    from pyflink.java_gateway import get_gateway
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment #, TableEnvironment
    # from pyflink.common import Configuration
except ModuleNotFoundError:
    print("Skipped flink import because of missing dependencies")
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery
    import ibis.backends.pyspark
except ModuleNotFoundError:
    print("Skipped spark import because of missing dependencies")
import ibis.backends
from ibis.backends.risingwave import Backend as RisingwaveBackend
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
        self.stream_size = 10
        self.did_create_sink = False

    @property
    def logger(self):
        return self.test_instance.benchmark
    
    def perform_measure_cached_to_none(self) -> tuple[float, float]:
        # by default, same behavior as perform_measure_to_none
        return self.perform_measure_to_none()
    
    def perform_measure_to_none(self) -> tuple[float, float]:
        def run():
            self.test_method()
            con = ibis.get_backend()
            con.execute(self.test_instance.query)
        return measure_time_memo(run)

    def perform_measure_to_file(self) -> tuple[float, float]:
        def run():
            self.test_method()
            con = ibis.get_backend()
            con.to_csv(self.test_instance.query, "out/ibis-backend-result.csv")
        return measure_time_memo(run)
    
    def cached_pre_query(self, table):
        # This is the fixed pre-query for scenario 3
        return (table
                .group_by(_.string1)
                .aggregate(int4=_.int4.sum(), int1=_.int1.max()))
    
    def preload_tables(self):
        con = ibis.get_backend()
        for name, table in self.test_instance.tables.items():
            self.test_instance.tables[name] = con.create_table(name, table, overwrite=True)

    def preload_tables_without_csv(self):
        # These backends don't allow reading from csv so self.tables is empty and we create it from scratch here.
        # Because the create table for these backends is extremely slow, we first check if the tables are
        # already in place and of the right size: if so, we skip the creation.
        con = ibis.get_backend()
        existing_tables = con.list_tables()
        tables = {}
        for name, file_path in self.test_instance.files.items():
            table = pd.read_csv(file_path)
            if name in existing_tables and con.table(name).count().execute() == table.shape[0]:
                tables[name] = con.table(name)
                continue
            print(f"Creating table {name} in {con.name} from {file_path}. Could take a while: might need to increase timeout...")
            tables[name] = con.create_table(name, table, overwrite=True)
        self.test_instance.tables = tables
    
    def preload_cached_query_from_tables(self):
        con = ibis.get_backend()
        def run():
            name = "ints_strings"
            table = self.test_instance.tables[name]
            # new modified table in place of previous one in tables (with old name) so transparent to next timed query
            # but with new name in db so we preserve standard dataset for slow loading backends
            self.test_instance.tables[name] = con.create_table(name + "_cached", self.cached_pre_query(table), overwrite=True)
            # alternative version for s3: using ibis-provided cache, unsupported by risingwave
            # self.test_instance.tables[name] = self.cached_pre_query(table).cache()
        return measure_time_memo(run)
    
    def preload_cached_query(self):
        self.preload_tables()
        return self.preload_cached_query_from_tables()
    
    def preload_cached_query_without_csv(self):
        self.preload_tables_without_csv()
        return self.preload_cached_query_from_tables()
    
    def perform_measure_cached_one_shot_to_none_from_tables(self) -> tuple[float, float]:
        def run():
            # save lazy pre-query instead of base table
            self.test_instance.tables["ints_strings"] = self.cached_pre_query(self.test_instance.tables["ints_strings"])
            # test method will compose rest of the query on top of pre-query
            self.test_method()
            con = ibis.get_backend()
            con.execute(self.test_instance.query)
        return measure_time_memo(run)
    
    def perform_measure_cached_one_shot_to_none(self) -> tuple[float, float]:
        self.preload_tables()
        return self.perform_measure_cached_one_shot_to_none_from_tables()
    
    def perform_measure_cached_one_shot_to_none_without_csv(self) -> tuple[float, float]:
        self.preload_tables_without_csv()
        return self.perform_measure_cached_one_shot_to_none_from_tables()

    def perform_measure_to_kafka(self) -> tuple[float, float]:
        self.test_method()
        self.create_view()
        # create sink needs to run in separate thread for pyflink otherwise it blocks
        self.subp = Thread(target=self.create_sink)
        self.subp.start()
        # wait for sink to be created before starting producer, so backends don't miss any
        # message, as they're set with latest read strategy,
        # then subp will keep running in case of pyflink to keep streaming query alive
        while not self.did_create_sink:
            time.sleep(1)
        return self.perform_measure_latency_kafka_to_kafka()

    def perform_measure_latency_kafka_to_kafka(self) -> tuple[float, float]:
        producer = Producer(self.test_instance.kafka_topic_name)
        consumer = Consumer()
        # starting the consumer in a separate thread so it doesn't "miss" the message
        # produced by the view from source to sink between the call to write_datum and read_datum
        # and passing self so it can toggle self.do_stop and stop the spark instance
        consumer_proc = Thread(target=consumer.read_data, args=[self])
        consumer_proc.start()
        start_time = time.perf_counter()
        # the backend is already set up to update its internal view and
        # write it to the sink topic
        producer.write_data(items=self.stream_size)
        # block until the consumer receives the result from sink
        consumer_proc.join()
        # once_consumer.proc returns, it must have toggled self.do_stop so
        # this join doesn't block anything
        self.subp.join()
        if consumer.did_read == False:
            raise Exception("No data in sink topic: either there's a failure or the query filters out everything")
        end_time = consumer.read_timestamp
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
        con = self.get_backend_con()
        ibis.set_backend(con)

    @staticmethod
    def get_backend_con() -> ibis.backends.flink.Backend:
        # connecting to a standalone flink cluster instead of the built-in one
        # instead of re-starting flink instance, cancel all jobs to avoid stuck jobs after failure
        subprocess.run("./benchmark/cancel_flink_jobs.sh", shell=True)
        gateway = get_gateway()
        string_class = gateway.jvm.java.lang.String
        string_array = gateway.new_array(string_class, 0)
        stream_env = gateway.jvm.org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
        j_stream_execution_environment = stream_env.createRemoteEnvironment(
            "localhost", 
            8081, 
            string_array)
        exec_env = StreamExecutionEnvironment(j_stream_execution_environment).set_parallelism(12)
        settings = (EnvironmentSettings.new_instance()
                    .in_streaming_mode()
                    .build())
        table_env = StreamTableEnvironment.create(
            exec_env,
            settings)
        return ibis.flink.connect(table_env)

class SparkBenchmark(BackendBenchmark):
    name = "spark"

    @staticmethod
    def get_backend_con() -> ibis.backends.pyspark.Backend:
        scala_version = '2.12'
        spark_version = '3.1.2'
        # ensure match above values match the correct versions in pip
        packages = [
            f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
            'org.apache.kafka:kafka-clients:3.2.1'
        ]
        session = SparkSession.builder\
            .master("spark://127.0.0.1:7077")\
            .appName("ibis")\
            .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
        try:
            # depending on Ibis version: 9.2 accepts mode parameter
            # while 8.0 doesn't
            return ibis.pyspark.connect(session, mode="streaming")
        except:
            return ibis.pyspark.connect(session)
    
    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        con = self.get_backend_con()
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
    
    def perform_measure_cached_one_shot_to_none(self) -> tuple[float, float]:
        return super().perform_measure_cached_one_shot_to_none_without_csv()

class RisingwaveBenchmark(BackendBenchmark):
    name = "risingwave"

    def __init__(self, test_instance: test.TestCompiler, test_method) -> None:
        super().__init__(test_instance, test_method)
        con = self.get_backend_con()
        ibis.set_backend(con)
        
    @staticmethod
    def get_backend_con() -> ibis.backends.risingwave.Backend:
        return ibis.risingwave.connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev",)

    def preload_cached_query(self):
        return super().preload_cached_query_without_csv()
    
    def perform_measure_cached_one_shot_to_none(self) -> tuple[float, float]:
        return super().perform_measure_cached_one_shot_to_none_without_csv()
    
    def create_view(self):
        con: RisingwaveBackend = ibis.get_backend()
        con.create_materialized_view("view_kafka",
                                     obj=self.test_instance.query,
                                     overwrite=True)

    def create_sink(self):
        con: RisingwaveBackend = ibis.get_backend()
        print(con.list_tables())
        con.create_sink("sink_kafka",
                        sink_from="view_kafka",
                        connector_properties={"connector": "kafka",
                                              "topic": "sink",
                                              "properties.bootstrap.server": "localhost:9092"},
                        data_format="PLAIN",
                        encode_format="JSON",
                        encode_properties={"force_append_only": "true"})
        self.did_create_sink = True
        print("Created risingwave view and sink")


def measure_time_memo(runnable, args=(), kwargs={}):
    start_time = time.perf_counter()
    memo = memory_usage((runnable, args, kwargs), include_children=True)
    end_time = time.perf_counter()
    return end_time - start_time, max(memo)