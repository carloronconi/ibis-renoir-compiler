import asyncio
import os
import subprocess
import sys
import time
import unittest
import ibis.backends
import ibis.backends.duckdb
import ibis.expr
import ibis.expr.types
import pandas as pd
import ibis
from difflib import unified_diff
from codegen import ROOT_DIR, Benchmark
from ibis import _
from pyflink.table import EnvironmentSettings, TableEnvironment
import os
import shutil
from codegen import compile_ibis_to_noir, compile_preloaded_tables_evcxr
from memory_profiler import memory_usage



class TestCompiler(unittest.TestCase):

    def __init__(self, methodName: str = "runTest"):
        """
        moved part of the setup to __init__ even though not customary for tests (both __init__ and setUp are 
        called before each test method) because we use the test case without unittest's harness for performance
        benchmarks
        """
        self.run_after_gen = os.getenv("RUN_AFTER_GEN", "true") == "true"
        self.render_query_graph = os.getenv(
            "RENDER_QUERY_GRAPH", "false") == "true"
        self.perform_assertions = os.getenv(
            "PERFORM_ASSERTIONS", "true") == "true"

        self._testMethodName = methodName
        # initialize benchmark data for current test name
        self.benchmark = Benchmark(methodName) if os.getenv(
            "PERFORM_BENCHMARK", "true") == "true" else None
        self.perform_compilation = True
        self.print_output_to_file = True
        self.renoir_cached = False

        super().__init__(methodName=methodName)

    def setUp(self):
        try:
            os.remove(ROOT_DIR + "/out/noir-result.csv")
        except FileNotFoundError:
            pass

    def complete_test_tasks(self, tab_name: str = None):
        # if tab_name defaults to None, selecting all tables
        if tab_name:
            files_tabs = [(self.files[tab_name], self.tables[tab_name])]
        else:
            files_tabs = [(self.files[k], self.tables[k]) for k in self.files.keys()]

        if self.perform_compilation:
            compile_ibis_to_noir(files_tabs,
                                 self.query, 
                                 self.run_after_gen, 
                                 self.print_output_to_file, 
                                 self.render_query_graph, 
                                 self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def init_benchmark_settings(self, perform_compilation: bool = False):
        self.run_after_gen = True
        self.render_query_graph = False
        self.perform_assertions = False
        self.perform_compilation = perform_compilation
        self.print_output_to_file = False

    def set_backend(self, backend: str, cached: bool):
        self.benchmark.backend_name = backend
        if backend == "renoir" or (backend == "duckdb" and not cached):
            # in-memory duckdb, for renoir it's used just to create the AST
            # while for duckdb it's used to store the tables
            ibis.set_backend("duckdb")
            # for cached renoir, we set here the flag
            self.renoir_cached = (backend == "renoir" and cached)
        elif backend == "duckdb" and cached:
            # in-memory duckdb instance
            ibis.set_backend(ibis.connect("duckdb://"))
        elif backend == "flink":
            table_env = TableEnvironment.create(
                EnvironmentSettings.in_streaming_mode())
            con = ibis.flink.connect(table_env)
            ibis.set_backend(con)
        elif backend == "polars":
            ibis.set_backend("polars")
        elif backend == "postgres":
            # when running `pip install -r requirements3.11.txt` a failure will occur with psycopg2
            # to fix it:
            # - `sudo apt install libpq-dev` which contains pg_config, `which pg_config` and `export PATH=/usr/bin/pg_config:$PATH`
            # - `sudo apt install python3.11-dev` which contains the python headers
            # - `pip install -r requirements3.11.txt`
            # before running tests with postgres, you also need to `docker compose -f benchmark/compose-postgres.yaml up`
            ibis.set_backend(ibis.postgres.connect(
                user="postgres",
                password="postgres",
                host="localhost",
                port=5432,
                database="postgres"))
        elif backend == "risingwave":
            ibis.set_backend(ibis.risingwave.connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev",))
        else:
            raise ValueError(
                f"Backend {backend} not supported - check if it requires special ibis setup before adding")

    def preload_tables(self, backend: str):
        if backend == "flink":
            # instead of wasting time running these backends both in "csv" and "cached" table origins
            # we raise an exception when trying to run in "cached" mode
            raise NotImplementedError(
                "Streaming backends don't allow preloading tables")

        if backend == "renoir":
            # here we create the two initialization files for evcxr:
            # - init.evcxr: for the imports, from cargo.toml
            # - preload_evcxr.rs: for reading tables from csv
            compile_preloaded_tables_evcxr([(self.files[k], self.tables[k]) for k in self.files.keys()])
            # note: we don't actually run the code we compiled into evcxr yet, we just write it to file here
            # so technically, we didn't preload the tables yet: well'do that in the test run, just before
            # starting the timer
            return

        con = ibis.get_backend()
        if backend == "postgres" or backend == "risingwave":
            # These backends don't allow reading from csv so self.tables is empty and we create it from scratch here.
            # Because the create table for these backends is extremely slow, we first check if the tables are
            # already in place and of the right size: if so, we skip the creation.
            self.tables = {}
            for name, file_path in self.files.items():
                if name in con.list_tables() and con.table(name).count().execute() == pd.read_csv(file_path).shape[0]:
                    self.tables[name] = con.table(name)
                    continue
                print(f"Creating table {name} in {backend} from {file_path}. Could take a while: might need to increase timeout...")
                self.tables[name] = con.create_table(name, pd.read_csv(file_path), overwrite=True)
            return
        # in `cached` mode we preload tables using create_table
        for name, table in self.tables.items():
            self.tables[name] = con.create_table(
                name, table.to_pandas(), overwrite=True)
            
    async def run_evcxr(self, test_method):
        # preloading the tables in evcxr ensuring the env var is set
        # to avoid re-compiling all dependencies
        proc = await asyncio.subprocess.create_subprocess_exec(
            "./benchmark/evcxr.sh", 
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
        )
        # read the welcome message
        await proc.stdout.readline()
        # load the imports and the table preloading function cache()
        with open(ROOT_DIR + "/noir_template/init.evcxr", 'r') as file:
            proc.stdin.write(file.read().encode())
        with open(ROOT_DIR + "/noir_template/preload_evcxr.rs", 'r') as file:
            proc.stdin.write(file.read().encode())
        # make sure that cache() has finished running by asking for :vars and waiting for the output
        proc.stdin.write(b":vars\n")
        # wait for the output of :vars to ensure that the tables are loaded before continuing
        # there should be 2 tables in the cache plus an empty line
        await proc.stdout.readline()
        await proc.stdout.readline()
        await proc.stdout.readline()


        # performing the actual timed query
        start_time = time.perf_counter()
        # test_method only compiles to main_evcxr.rs, then we run it in evcxr
        memo = memory_usage((test_method,), include_children=True)
        with open(ROOT_DIR + "/noir_template/src/main_evcxr.rs", 'r') as file:
            proc.stdin.write(file.read().encode())

        # no output is visible, so we check for failures by detecting the new var "result"
        # created if the process didn't panic, plus the additional newline
        proc.stdin.write(b":vars\n")
        success = "result: bool" in (await proc.stdout.readline()).decode("utf-8")
        await proc.stdout.readline()
        proc.stdin.write(b":quit\n")
        end_time = time.perf_counter()

        if not success:
            raise Exception("Noir code panicked within evcxr!")
        return memo, end_time - start_time

    def create_files_no_headers(self) -> dict[str, str]:
        suffix = "_no_headers"
        no_header_files = {}
        for name, file_path in self.files.items():
            no_header_path = file_path.replace(".csv", suffix + ".csv")
            if os.path.isfile(no_header_path):
                no_header_files[name] = no_header_path
                continue
            with open(file_path, 'r') as f_in:
                with open(no_header_path, 'w') as f_out:
                    next(f_in)  # skip first line
                    for line in f_in:
                        f_out.write(line)
            no_header_files[name] = no_header_path
        return no_header_files

    def init_files(self, file_suffix=""):
        raise NotImplementedError

    def init_tables(self):
        raise NotImplementedError

    def tearDown(self) -> None:
        if not self.benchmark:
            return
        # only run ibis query if not already run in assert_similarity_noir_output
        if not hasattr(self.benchmark, "ibis_time_s"):
            self.run_ibis_query()
        self.benchmark.log()

    def run_ibis_query(self):

        # benchmark ibis total run time + write to csv (as noir also performs write to csv)
        start_time = time.perf_counter()

        # for non-nullable tests rebuild query over new memtable as self.tables is non-materialized
        # to be able to define its schema as non-nullable (ibis doesn't allow defining schema when reading from csv)
        if hasattr(self, "query_func"):
            df_left = pd.read_csv(self.files["fruit_left"])
            df_right = pd.read_csv(self.files["fruit_right"])
            tables = {"fruit_left": ibis.memtable(df_left, schema=self.schema),
                      "fruit_right": ibis.memtable(df_right, schema=self.schema)}
            self.query = self.query_func(tables)

        self.df_ibis = self.query.to_pandas()
        directory = ROOT_DIR + "/out"
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.df_ibis.to_csv(directory + "/ibis-benchmark.csv")
        end_time = time.perf_counter()
        self.benchmark.ibis_time_s = end_time - start_time

    def assert_equality_noir_source(self):
        test_expected_file = "/test/expected/" + \
            sys._getframe().f_back.f_code.co_name + ".rs"

        with open(ROOT_DIR + test_expected_file, "r") as f:
            expected_lines = f.readlines()
        with open(ROOT_DIR + "/noir_template/src/main.rs", "r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        self.assertEqual(diff, [], "Differences:\n" + "".join(diff))
        print("\033[92m Source equality: OK\033[00m")

    def assert_similarity_noir_output(self, noir_subset_ibis=False):
        self.run_ibis_query()

        self.round_float_cols(self.df_ibis)
        df_ibis = self.df_ibis
        df_ibis.to_csv(ROOT_DIR + "/out/ibis-result.csv")

        noir_path = ROOT_DIR + "/out/noir-result.csv"
        # if noir file has size 0 it means no output rows were generated by the query and read_csv will fail
        # this happens because noir doesn't output the header row when output has 0 rows (while ibis does), so we need to
        # consider df_noir as an empty dataframe, and just check that df_ibis is empty
        if os.path.getsize(noir_path) == 0:
            self.assertEqual(len(df_ibis.index), 0,
                             "Noir output is 0 rows, while ibis is not!")
            return

        df_noir = pd.read_csv(noir_path)
        self.round_float_cols(df_noir)

        # with keyed streams, noir preserves the key column with its original name
        # with joins, both the key column and the corresponding cols in joined tables are preserved
        # with outer joins, the left preserved col could have NaNs that the key doesn't have, so drop the key col and
        # preserve left joined col instead
        noir_cols = list(df_noir.columns)
        if len(noir_cols) > 1 and noir_cols[1] == noir_cols[0] + ".1":
            df_noir.drop(noir_cols[0], axis=1, inplace=True)
            df_noir.rename(columns={noir_cols[1]: noir_cols[0]}, inplace=True)

        # noir can output duplicate columns and additional columns, so remove duplicates and select those in ibis output
        df_noir = df_noir.loc[:, ~df_noir.columns.duplicated(
        )][df_ibis.columns.tolist()]

        # dataframes now should be exactly the same aside from row ordering:
        # group by all columns and count occurrences of each row
        df_ibis = df_ibis.groupby(df_ibis.columns.tolist(
        ), dropna=False).size().reset_index(name="count")
        df_noir = df_noir.groupby(df_noir.columns.tolist(
        ), dropna=False).size().reset_index(name="count")

        # fast fail if occurrence counts have different lengths
        if not noir_subset_ibis:
            self.assertEqual(len(df_ibis.index), len(df_noir.index),
                             f"Row occurrence count tables must have same length! Got this instead:\n{df_ibis}\n{df_noir}")

        # occurrence count rows could still be in different order so use a join on all columns
        join = pd.merge(df_ibis, df_noir, how="outer",
                        on=df_ibis.columns.tolist(), indicator=True)
        both_count = join["_merge"].value_counts()["both"]
        join.to_csv(ROOT_DIR + "/out/ibis-noir-comparison.csv")

        if not noir_subset_ibis:
            self.assertEqual(both_count, len(join.index),
                             f"Row occurrence count tables must have same values! Got this instead:\n{join}")
        else:
            # here we allow for noir to output fewer rows than ibis
            # used for windowing, where ibis semantics don't include windows with size
            # smaller than specified, while noir does
            left_count = join["_merge"].value_counts()["left_only"]
            right_count = join["_merge"].value_counts()["right_only"]
            message = f"Noir output must be a subset of ibis output! Got this instead:\n{join}"
            self.assertGreaterEqual(left_count, 0, message)
            # only reason why a right_only row should exist is if an identical left_only row with a higher count exists
            if right_count > 0:
                cols = join.columns.tolist()
                cols.remove("count")
                cols.remove("_merge")
                for _, r_row in join[join["_merge"] == "right_only"].iterrows():
                    found = False
                    for _, l_row in join[join["_merge"] == "left_only"].iterrows():
                        if all(r_row[col] == l_row[col] for col in cols) and r_row["count"] < l_row["count"]:
                            found = True
                            break
                    self.assertTrue(found, message)
            else:
                self.assertEqual(right_count, 0, message)
            self.assertGreaterEqual(both_count, 0, message)

        print(f"\033[92m Output similarity: OK\033[00m")

    @staticmethod
    def round_float_cols(df: pd.DataFrame, decimals=3):
        for i, t in enumerate(df.dtypes):
            if t == "float64":
                df.iloc[:, i] = df.iloc[:, i].round(decimals)
