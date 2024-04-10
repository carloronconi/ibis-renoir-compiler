import os
import shutil
import unittest

import test
from codegen import ROOT_DIR


def update_all_expected_sources():
    """
    Updates all expected sources of tests with whatever the tests currently produce
    Useful when made a change that changes expected noir code of all tests and already ensured output similarity
    """
    tests = [(t._testMethodName + ".rs", t) for t in list(unittest.findTestCases(test))[0]._tests]
    for file, t in tests:
        file = ROOT_DIR + "/test/expected/" + file
        t.run()

        # replace old expected file with file produced by test
        try:
            os.remove(file)
        except FileNotFoundError:
            pass  # ignore: create new file
        shutil.copyfile(ROOT_DIR + "/noir-template/src/main.rs", file)


if __name__ == "__main__":
    update_all_expected_sources()
