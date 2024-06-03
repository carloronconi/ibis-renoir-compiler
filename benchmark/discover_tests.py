import subprocess
import unittest
import test

def main():
    test_classes = test.TestCompiler.__subclasses__()
    test_suites = [unittest.TestLoader().loadTestsFromTestCase(c) for c in test_classes]
    test_cases = [t.id() for s in test_suites for t in s]

    # for t in test_cases:
    #     name = t.split(".")[-1]
    #     subprocess.run(f"command time -a -o \"log/memo_log.csv\" -f \"{name}\",\"%e\",\"%M\",\"%U\",\"%S\",\"%x\" python -m unittest {t}", shell=True)

    # f = open("test_names.txt", "w")
    # [f.write(f"{t}\n") for t in test_cases]
    # f.close()

    [print(s) for s in test_cases]

if __name__ == "__main__":
    main()