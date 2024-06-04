import unittest
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
    parser = argparse.ArgumentParser("ibis_quickstart")
    parser.add_argument("test_case", help="Which testcase to run. Use `python -m benchmark.discover_tests` to see the current list of tests.", type=str)
    parser.add_argument("--paths", help="Paths to the files to be used in the test.", nargs="+", required=True, type=str)
    args = parser.parse_args()

    test_class, test_case = args.test_case.rsplit(".", 1)
    
    test_instance = eval(f"{test_class}()")
    test_instance.files = args.paths
    test_instance.tables = [ibis.read_csv(file) for file in test_instance.files]
    test_instance.run_after_gen = True
    test_instance.render_query_graph = False
    test_instance.benchmark = None
    test_instance.perform_assertions = False
    eval(f"test_instance.{test_case}()", {"test_instance": test_instance})

if __name__ == "__main__":
    main()