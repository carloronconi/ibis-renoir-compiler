import benchmark.load_tests as bench
import test
import argparse
import test.test_nexmark
import test.test_operators
import ibis
from pyflink.table import EnvironmentSettings, TableEnvironment


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
                        type=str, choices=["csv", "backend"],  default="csv")
    args = parser.parse_args()

    tests_full = [t for t in bench.main() if args.pattern in t]
    tests_split: list[tuple] = [t.rsplit(".", 1) for t in tests_full]

    for test_class, test_case in tests_split:
        for backend in args.backends:
            test_instance = eval(f"{test_class}(\"{test_case}\")")
            # in case the backend is renoir, we leave the default duckdb backend to load the tables
            # otherwise, we load the tables with the desired one
            if backend != "renoir":
                ibis.set_backend(backend)
            # TODO: not sure of what the intended process should be in the two cases
            if args.table_origin == "backend":
                test_instance.init_table_files(file_suffix=args.path_suffix,
                                               skip_tables=(args.table_origin == "backend" and args.backend != "renoir"))
                
    # TODO: old stuff from __main__.py to be converted
    # renoir is tested in the same way regardless of the table_origin: we always load from csv
    test_instance.init_table_files(file_suffix=args.path_suffix, skip_tables=(
        args.table_origin == "backend" and args.backend != "renoir"))
    if args.table_origin == "load":
        test_instance.store_tables_in_backend(args.backend)
        return
    test_instance.run_after_gen = True
    test_instance.render_query_graph = False
    # leaving logging on doesn't seem to affect performance
    # test_instance.benchmark = None
    test_instance.perform_assertions = False
    test_instance.perform_compilation = True if args.backend == "renoir" else False
    # when running performance benchmarks, don't write to file
    test_instance.print_output_to_file = False
    if args.table_origin == "backend":
        test_instance.read_tables_from_backend(args.backend)

    eval(f"test_instance.{test_case}()", {"test_instance": test_instance})
    # If the backend is renoir, we have already performed the compilation to renoir code and ran it
    if (args.backend == "renoir"):
        if (test_instance.benchmark is not None):
            test_instance.benchmark.log()
        return

    # Else, the test we ran simply populated the query attribute with the ibis query
    # and we can run it using to_pandas(), after setting the backend to the desired one
    # Flink requires special setup
    if (args.backend == "flink"):
        table_env = TableEnvironment.create(
            EnvironmentSettings.in_streaming_mode())
        con = ibis.flink.connect(table_env)
        ibis.set_backend(con)
    else:
        if args.table_origin == "csv":
            ibis.set_backend(args.backend)
        else:
            ibis.set_backend(ibis.connect(
                f"{args.backend}://{args.backend}.db"))
    test_instance.query.to_pandas().head()
    print(f"finished running query with: {ibis.get_backend().name}")


if __name__ == "__main__":
    main()
