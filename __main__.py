import test
import argparse
import test.test_nexmark
import test.test_operators
import ibis
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    # Example standalone usage:
    # python ../ibis-renoir-compiler test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-renoir-compiler/data/int-1-string-1.csv /home/carlo/Projects/ibis-renoir-compiler/data/int-3.csv
    # Example hyperfine usage:
    # hyperfine --warmup 5 "python ../ibis-renoir-compiler test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-renoir-compiler/data/int-1-string-1.csv /home/carlo/Projects/ibis-renoir-compiler/data/int-3.csv" --export-json result.json
    # Example hyperfine usage comparing renoir and polars:
    # hyperfine --warmup 5 "python ../ibis-renoir-compiler test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-renoir-compiler/data/int-1-string-1.csv /home/carlo/Projects/ibis-renoir-compiler/data/int-3.csv --backend renoir" "python ../ibis-renoir-compiler test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-renoir-compiler/data/int-1-string-1.csv /home/carlo/Projects/ibis-renoir-compiler/data/int-3.csv --backend polars" --export-json result.json
    parser = argparse.ArgumentParser("ibis-renoir-compiler")
    parser.add_argument("test_case", 
                        help="Which testcase to run. Use `python -m benchmark.discover_tests` to see the current list of tests.", type=str)
    parser.add_argument("--path_suffix", 
                        help="Suffix for test files used by test_case. Useful for having same file with growing sizes.", default="", type=str)
    parser.add_argument("--backend", 
                        help="Which backend to use.", type=str, choices=["renoir", "duckdb", "snowflake", "flink", "polars"], default="renoir")
    parser.add_argument("--table_origin", 
                        help="Instead of running the query starting from the csv load, read it directly from backend table. Before running with 'backend', run once with'load' to store the required tables in the backend", 
                        type=str, choices=["csv", "load", "backend"],  default="csv")
    args = parser.parse_args()

    test_class, test_case = args.test_case.rsplit(".", 1)

    # because we're not using unittest's harness, we need to set the method name manually
    test_instance = eval(f"{test_class}(\"{test_case}\")")
    test_instance.init_table_files(file_suffix=args.path_suffix)
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
            ibis.set_backend(ibis.connect(f"{args.backend}://{args.backend}.db"))
    test_instance.query.to_pandas().head()
    print(f"finished running query with: {ibis.get_backend().name}")


if __name__ == "__main__":
    main()
