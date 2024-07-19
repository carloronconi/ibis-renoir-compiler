import unittest
import test

def main() -> list[str]:
    test_classes = get_subclasses(test.TestCompiler)
    test_suites = [unittest.TestLoader().loadTestsFromTestCase(c) for c in test_classes]
    test_cases = [t.id() for s in test_suites for t in s]

    return test_cases

def get_subclasses(cls):
    for subclass in cls.__subclasses__():
        yield from get_subclasses(subclass)
        yield subclass

if __name__ == "__main__":
    main()