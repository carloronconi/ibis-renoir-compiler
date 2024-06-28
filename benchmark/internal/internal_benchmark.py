import multiprocessing.connection
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
import traceback


TIMEOUT_S = 60 * 5  # 5 minutes


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
                main, worker = multiprocessing.Pipe(duplex=True)
                p = multiprocessing.Process(target=child_workload, args=(
                    worker, test_class, test_case, backend, table_origin, args.path_suffix, args.runs, args.warmup, args.dir))
                p.start()
                count = args.warmup + args.runs
                allow_runs(count, p, main, test_case, backend, table_origin, args.dir)


def allow_runs(count: int, p: multiprocessing.Process, conn: multiprocessing.connection.Connection, test_case: str, backend: str, table_origin: str, dir: str):
    """
    send messages to worker allowing it to perform run_once, and kill it if it takes too long
    """
    for i in range(count):
        conn.send("go")
        if not conn.poll(TIMEOUT_S):
            p.kill()
            p.join()
            # if the process was killed, we assume it didn't log and log from this main thread instead
            logger = bm.Benchmark(test_case, dir)
            logger.backend_name = backend
            logger.table_origin = table_origin
            logger.run_count = i
            logger.exception = "timeout"
            logger.log()
            print(f"timeout: killed process with backend {backend} and origin {table_origin} at run {i}")
            return
        success, message = conn.recv()
        if not success:
            print("exception: " + message)
            return
        print("success: " + message)


def child_workload(pipe: multiprocessing.connection.Connection, test_class: str, test_case: str, backend: str, table_origin: str, path_suffix: str, runs: int, warmup: int, dir: str):
    try:
        test_instance: test.TestCompiler = eval(
            f"{test_class}(\"{test_case}\")")
        test_instance.benchmark = bm.Benchmark(
            test_case, dir)
        test_instance.benchmark.table_origin = table_origin

        # in case the backend is renoir, we leave the default duckdb backend to read the tables to create the AST
        # otherwise, we load the tables with the desired one
        test_instance.set_backend(
            backend, cached=(table_origin == "cached"))

        test_instance.init_benchmark_settings(
            perform_compilation=(backend == "renoir"))

        test_instance.init_files(file_suffix=path_suffix)
        test_instance.init_tables()

        # if table origin is cached, we need to pre-load the tables in the backends before submitting the queries
        # otherwise, we measure the time of both loading the table and running the query
        if table_origin == "cached":
            test_instance.preload_tables(backend)

        for i in range(warmup + runs):
            count = i - warmup if i >= warmup else -1
            # wait for permission from main thread
            # which starts counting down before sending the message so it can kill this process in case it hangs
            pipe.recv()
            message = run_once(test_case, test_instance, count, backend)
            # telling the main thread not to kill this process
            pipe.send((True, message))
    except Exception as e:
        trace = " ".join(traceback.format_exception(e))
        test_instance.benchmark.exception = trace
        test_instance.benchmark.log()
        pipe.send((False, trace))


def run_once(test_case: str, test_instance: test.TestCompiler, run_count: int, backend: str) -> str:
    test_instance.benchmark.run_count = run_count
    test_instance.benchmark.backend_name = backend

    start_memo = process_memory()
    start_time = time.perf_counter()

    test_method = getattr(test_instance, test_case)
    test_method()
    # If the backend is renoir, we have already performed the compilation to renoir code and ran it after this line
    end_memo = process_memory()

    if backend != "renoir":
        test_instance.query.execute()
        end_memo = process_memory()

    end_time = time.perf_counter()
    total_time = end_time - start_time
    test_instance.benchmark.total_time_s = total_time
    total_memo = end_memo - start_memo
    test_instance.benchmark.max_memory_B = total_memo
    test_instance.benchmark.log()
    return f"ran once - backend: {backend}\trun: {run_count:03}\ttime: {total_time:.10f}\tquery: {test_case}"


def process_memory():
    process = psutil.Process(os.getpid())
    memo = process.memory_info().rss
    for child in process.children(recursive=True):
        memo += child.memory_info().rss
    return memo


if __name__ == "__main__":
    main()
