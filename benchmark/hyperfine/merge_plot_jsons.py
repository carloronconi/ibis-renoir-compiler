import subprocess
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
        if args.pattern in file and not "COMPARISON" in file:
            files.append(os.path.join(args.dir, file))

    common_names = dict()
    for file in files:
        common_name, _ = file.rsplit("_", 1)
        full_names = common_names.get(common_name, [])
        full_names.append(file)
        common_names[common_name] = full_names

    for common_name, full_names in common_names.items():
        out_name = common_name + "_COMPARISON.json"
        out_content = ""
        
        out_content += "{\n\"results\": ["
        for i, file in enumerate(full_names):
            # print(common_name, file)
            with open(file, "r") as in_file:
                try:
                    content = in_file.read()
                except:
                    continue
                content = content.split("[", 1)[-1]
                content = content.rsplit("]", 1)[0]
                out_content += content
            if i < len(full_names) - 1:
                out_content += ","
        out_content += "]\n}"
        with open(out_name, "w") as out_file:
            out_file.write(out_content)

    for common_name, full_names in common_names.items():
        out_name = common_name + "_COMPARISON.json"
        backends = [name.split("_")[-1].split(".")[0] for name in full_names].__str__().replace("[", "").replace("]", "").replace(" ", "").replace("'", "")
        subprocess.run(f"python -m benchmark.hyperfine_scripts.plot_whisker {out_name} --output {common_name}_COMPARISON.png --labels {backends} --sort-by command", shell=True)

    
if __name__ == "__main__":
    main()
