import benchmark.load_tests as bench
import test
import argparse
import test.test_nexmark
import test.test_operators
import ibis
import time


def main():
    parser = argparse.ArgumentParser("ibis-renoir-compiler")
    parser.add_argument("--test_pattern",
                        help="Pattern to select which tests to run among those discoverable by unittest. By default all are included",
                        default="", type=str)
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
                        type=list[str], default=["duckdb", "flink", "polars", "renoir"])
    parser.add_argument("--table_origin",
                        help="Instead of running the query starting from the csv load, read it directly from backend table. \
                              No need to perform load as instrumented run can load before running without affecting the measured data",
                        type=str, choices=["csv", "cached"],  default="csv")
    args = parser.parse_args()

    tests_full = [t for t in bench.main() if args.pattern in t]
    tests_split: list[tuple] = [t.rsplit(".", 1) for t in tests_full]

    for test_class, test_case in tests_split:
        for backend in args.backends:
            test_instance: test.TestCompiler = eval(
                f"{test_class}(\"{test_case}\")")

            # in case the backend is renoir, we leave the default duckdb backend to read the tables to create the AST
            # otherwise, we load the tables with the desired one
            test_instance.set_backend(
                backend, cached=args.table_origin == "cached")

            test_instance.init_benchmark_settings(
                perform_compilation=(backend == "renoir"))

            # if table origin is cached, we need to pre-load the tables in the backends before submitting the queries
            # otherwise, we measure the time of both loading the table and running the query
            test_instance.init_table_files(file_suffix=args.path_suffix,
                                           skip_tables=(args.table_origin == "cached"))
            if args.table_origin == "cached":
                test_instance.preload_tables(backend)

            for _ in range(args.warmup):
                run_once(test_case, test_instance, -1)

            for i in range(args.runs):
                run_once(test_case, test_instance, i)


def run_once(test_case: str, test_instance: test.TestCompiler, run_count: int, backend: str):
    test_instance.benchmark.run_count = run_count
    test_instance.benchmark.backend_name = backend

    print(f"running query with with: {ibis.get_backend().name}")

    start_time = time.perf_counter()
    eval(f"test_instance.{test_case}()", {"test_instance": test_instance})
    # If the backend is renoir, we have already performed the compilation to renoir code and ran it
    # after this line

    if backend != "renoir":
        test_instance.query.execute()

    end_time = time.perf_counter()
    test_instance.benchmark.total_time = end_time - start_time
    test_instance.benchmark.log()


if __name__ == "__main__":
    main()
