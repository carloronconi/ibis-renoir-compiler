import ibis
from test.test_operators import TestCompiler
from codegen import ROOT_DIR



class TestNexmark(TestCompiler):
    def setUp(self):
        names = ["auction", "bid", "person"]
        file_prefix = ROOT_DIR + "/data/nexmark/"
        file_suffix = ".csv"

        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}
        self.tables = {n: ibis.read_csv(f) for n, f in self.files.items()}

        super().setUp()

    """
    Query 1
    ```
    SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
    FROM bid [ROWS UNBOUNDED];
    ```
    """
    def test_query1(self):
        pass
