import test
import argparse
import test.test_nexmark
import test.test_operators
import ibis
from pyflink.table import EnvironmentSettings, TableEnvironment
import os


def main():
    parser = argparse.ArgumentParser("merge_jsons")
    parser.add_argument(
        "--dir", help="Where to run merger.", type=str)
    parser.add_argument(
        "--pattern", help="Which pattern to use to find json benchmark results.", type=str)
    args = parser.parse_args()

    files = []
    for file in os.listdir(args.dir):
        if args.pattern in file:
            files.append(os.path.join(args.dir, file))

    common_names = dict()
    for file in files:
        common_name, _ = file.rsplit("_", 1)
        full_names = common_names.get(common_name, [])
        full_names.append(file)
        common_names[common_name] = full_names

    for common_name, full_names in common_names.items():
        with open(common_name + "_COMPARISON.json", "w") as outfile:
            outfile.write("{\n\"results\": [")
            for i, file in enumerate(full_names):
                # print(common_name, file)
                with open(file, "r") as infile:
                    content = infile.read()
                    content = content.split("[", 1)[-1]
                    content = content.rsplit("]", 1)[0]
                    outfile.write(content)
                    if i != len(full_names) - 1:
                        outfile.write(",")
            outfile.write("]\n}")

    
if __name__ == "__main__":
    main()
