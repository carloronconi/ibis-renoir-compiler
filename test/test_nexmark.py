import ibis
from codegen.generator import compile_ibis_to_noir
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

    def test_query1(self):
        """
        Query 1
        ```
        SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
        FROM bid [ROWS UNBOUNDED];
        ```
        """
        bid = self.tables["bid"]
        query = (bid
                 .mutate(dol_price=bid["price"] * 0.85)
                 .select(["auction", "price", "dol_price", "bidder", "date_time"]))

        compile_ibis_to_noir([(self.files["bid"], bid)],
                             query, run_after_gen=True, render_query_graph=False)

        # output is the same but some rows are considered different due to float precision
        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_query2(self):
        """
        SELECT Rstream(auction, price)
        FROM Bid [NOW]
        WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
        """

        bid = self.tables["bid"]
        query = (bid
                 .filter((bid["auction"] == 1007) | (bid["auction"] == 1020) | (bid["auction"] == 2001) | (bid["auction"] == 2019) | (bid["auction"] == 2087))
                 .select(["auction", "price"]))
        
        compile_ibis_to_noir([(self.files["bid"], bid)],
                             query, run_after_gen=True, render_query_graph=True)
        
        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()
