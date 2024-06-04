import test
import argparse
import test.test_nexmark
import test.test_operators
import ibis

def main():
    # Example standalone usage: 
    # python ../ibis-quickstart test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv
    # Example hyperfine usage:
    # hyperfine --warmup 5 "python ../ibis-quickstart test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv" --export-json result.json
    # Example hyperfine usage comparing renoir and polars:
    # hyperfine --warmup 5 "python ../ibis-quickstart test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv --backend renoir" "python ../ibis-quickstart test.test_operators.TestNullableOperators.test_nullable_filter_filter_select_select --paths /home/carlo/Projects/ibis-quickstart/data/int-1-string-1.csv /home/carlo/Projects/ibis-quickstart/data/int-3.csv --backend polars" --export-json result.json
    parser = argparse.ArgumentParser("ibis_quickstart")
    parser.add_argument("test_case", help="Which testcase to run. Use `python -m benchmark.discover_tests` to see the current list of tests.", type=str)
    parser.add_argument("--paths", help="Paths to the files to be used in the test.", nargs="+", required=True, type=str)
    parser.add_argument("--backend", help="Which backend to use.", type=str, choices=["renoir", "duckdb", "snowflake", "flink", "polars"], default="renoir")
    args = parser.parse_args()

    test_class, test_case = args.test_case.rsplit(".", 1)
    
    test_instance = eval(f"{test_class}()")
    test_instance.files = args.paths
    test_instance.tables = [ibis.read_csv(file) for file in test_instance.files]
    test_instance.run_after_gen = True
    test_instance.render_query_graph = False
    test_instance.benchmark = None
    test_instance.perform_assertions = False
    test_instance.perform_compilation = True if args.backend == "renoir" else False

    eval(f"test_instance.{test_case}()", {"test_instance": test_instance})
    # If the backend is renoir, we have already performed the compilation to renoir code and ran it
    if (args.backend == "renoir"):
        return
    
    # Else, the test we ran simply populated the query attribute with the ibis query
    # and we can run it using to_pandas(), after setting the backend to the desired one
    ibis.set_backend(args.backend)
    test_instance.query.to_pandas().head()
    print(f"finished running query with: {ibis.get_backend().name}")

if __name__ == "__main__":
    main()