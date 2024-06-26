import benchmark.discover.load_tests as bench
import test
import argparse
import test.test_nexmark
import test.test_operators
import time
import codegen.benchmark as bm
from datetime import datetime
import os
import psutil
import multiprocessing


RUN_ONCE_TIMEOUT_S = 60 * 5  # 5 minutes


def main():
    parser = argparse.ArgumentParser("ibis-renoir-compiler")
    parser.add_argument("--test_patterns",
                        help="Pattern to select which tests to run among those discoverable by unittest. By default all are included",
                        default=[""], type=str, nargs='+')
    parser.add_argument("--runs",
                        help="Number of runs to perform for each test. Defaults to 10",
                        type=int, default=10)
    parser.add_argument("--warmup",
                        help="Number of warmup runs to perform for each test. Defaults to 1",
                        type=int, default=1)
    parser.add_argument("--path_suffix",
                        help="Suffix for test files used by test_case. Useful for having same file with growing sizes.",
                        default="", type=str)
    parser.add_argument("--backends",
                        help="List of backends to use among duckdb, flink, polars, renoir. Defaults to all.",
                        type=str, nargs='+', default=["duckdb", "flink", "polars", "renoir"])
    parser.add_argument("--table_origin",
                        help="Instead of running the query starting from the csv load, read it directly from backend table. \
                              No need to perform load as instrumented run can load before running without affecting the measured data",
                        type=str, nargs='+', choices=["csv", "cached"],  default="csv")
    parser.add_argument("--dir",
                        help="Where to store the log file. Defaults to directory from timestamp.",
                        type=str, default=datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))
    args = parser.parse_args()

    tests_full = [t for t in bench.main() if any(
        pat in t for pat in args.test_patterns)]
    tests_split: list[tuple] = [t.rsplit(".", 1) for t in tests_full]

    for test_class, test_case in tests_split:
        for backend in args.backends:
            for table_origin in args.table_origin:
                try:
                    test_instance: test.TestCompiler = eval(
                        f"{test_class}(\"{test_case}\")")
                    test_instance.benchmark = bm.Benchmark(test_case, args.dir)
                    test_instance.benchmark.table_origin = table_origin

                    # in case the backend is renoir, we leave the default duckdb backend to read the tables to create the AST
                    # otherwise, we load the tables with the desired one
                    test_instance.set_backend(
                        backend, cached=table_origin == "cached")

                    test_instance.init_benchmark_settings(
                        perform_compilation=(backend == "renoir"))

                    test_instance.init_files(file_suffix=args.path_suffix)
                    if backend == "flink":
                        # flink doesn't support csv files with headers
                        test_instance.chop_file_headers()
                    test_instance.init_tables()

                    # if table origin is cached, we need to pre-load the tables in the backends before submitting the queries
                    # otherwise, we measure the time of both loading the table and running the query
                    if table_origin == "cached":
                        test_instance.preload_tables(backend)


                    for i in range(args.warmup + args.runs):
                        count = i - args.warmup if i >= args.warmup else -1
                        run_once(test_case, test_instance, count, backend)
                except Exception as e:
                    print_and_log(backend, test_case, table_origin,
                                  test_instance.benchmark, e)
                finally:
                    if backend == "flink":
                        test_instance.restore_file_headers()


def run_once(test_case: str, test_instance: test.TestCompiler, run_count: int, backend: str):
    test_instance.benchmark.run_count = run_count
    test_instance.benchmark.backend_name = backend

    start_memo = process_memory()
    start_time = time.perf_counter()

    run_timed(eval(f"test_instance.{test_case}()", {"test_instance": test_instance}),
              RUN_ONCE_TIMEOUT_S)
    # If the backend is renoir, we have already performed the compilation to renoir code and ran it
    # after this line

    end_memo = process_memory()

    if backend != "renoir":
        run_timed(test_instance.query.execute, RUN_ONCE_TIMEOUT_S)
        end_memo = process_memory()

    end_time = time.perf_counter()
    total_time = end_time - start_time
    test_instance.benchmark.total_time_s = total_time
    total_memo = end_memo - start_memo
    test_instance.benchmark.max_memory_B = total_memo
    test_instance.benchmark.log()
    print(
        f"ran once - backend: {backend}\trun: {run_count:03}\ttime: {total_time:.10f}\tquery: {test_case}")


def print_and_log(backend, test_case, table_origin, benchmark, exception):
    """
    Looks like a code smell, but actually we want to capture exceptions and log them instead of crashing the whole
    benchmarking process, so we can collect data from other tests even if one fails.
    """
    print(
        f"failed once - backend: {backend}\ttable origin: {table_origin}\tquery: {test_case}\texception: {exception.__class__.__name__}\n{exception}")
    benchmark.exception = exception
    benchmark.log()


def run_timed(func, timeout):
    """
    Runs the given function with a timeout in seconds, killing it if it takes too long.
    Useful for running tests that might hang: I'm looking at you, Flink.
    """
    p = multiprocessing.Process(target=func)
    p.start()
    # main thread waits for the timeout or the process to finish
    p.join(timeout)
    if p.is_alive():
        # if process still running after timeout, kill it
        p.kill()
        p.join()
        raise TimeoutError(
            f"run_timed killed function `{func.__name__}` after timeout of {timeout}s - skipping other runs with same combination of test_case, backend, table_origin")


def process_memory():
    process = psutil.Process(os.getpid())
    memo = process.memory_info().rss
    for child in process.children(recursive=True):
        memo += child.memory_info().rss
    return memo


if __name__ == "__main__":
    main()
