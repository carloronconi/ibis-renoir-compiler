import benchmark.discover.load_tests as bench

def main():
    test_cases = bench.main()

    [print(s) for s in test_cases]

if __name__ == "__main__":
    main()