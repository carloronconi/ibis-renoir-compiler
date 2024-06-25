import unittest
import test

def main() -> list[str]:
    test_classes = test.TestCompiler.__subclasses__()
    test_suites = [unittest.TestLoader().loadTestsFromTestCase(c) for c in test_classes]
    test_cases = [t.id() for s in test_suites for t in s]

    return test_cases

if __name__ == "__main__":
    main()