import ibis
from codegen.generator import compile_ibis_to_noir
from test.test_operators import TestCompiler
from codegen import ROOT_DIR
from ibis import _


class TestNexmark(TestCompiler):
    CURRENT_TIME = 2330277279926

    def setUp(self):
        names = ["auction", "bid", "person"]
        file_prefix = ROOT_DIR + "/data/nexmark/"
        file_suffix = ".csv"

        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}
        self.tables = {n: ibis.read_csv(f) for n, f in self.files.items()}

        super().setUp()

    def test_nexmark_query_1(self):
        """
        SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
        FROM bid [ROWS UNBOUNDED];
        """

        bid = self.tables["bid"]
        query = (bid
                 .mutate(dol_price=bid["price"] * 0.85)
                 .select(["auction", "price", "dol_price", "bidder", "date_time"]))

        compile_ibis_to_noir([(self.files["bid"], bid)],
                             query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_nexmark_query_2(self):
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
                             query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_nexmark_query_3(self):
        """
        SELECT Istream(P.name, P.city, P.state, A.id)
        FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
        """

        auction = self.tables["auction"]
        person = self.tables["person"]
        query = (auction
                 .join(person, auction["seller"] == person["id"])
                 .filter((person["state"] == "OR") | (person["state"] == "ID") | (person["state"] == "CA"))
                 .filter(auction["category"] == 10)
                 .select(["name", "city", "state", "id"]))
        compile_ibis_to_noir([(self.files["auction"], auction), (self.files["person"], person)],
                             query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_nexmark_query_4(self):
        """
        SELECT Istream(AVG(Q.final))
        FROM Category C, (SELECT Rstream(MAX(B.price) AS final, A.category)
                          FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
                          WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
                          GROUP BY A.id, A.category) Q
        WHERE Q.category = C.id
        GROUP BY C.id;
        """

        auction = self.tables["auction"]
        bid = self.tables["bid"]
        query = (auction
                 .join(bid, auction["id"] == bid["auction"])
                 .filter(bid["date_time"] < auction["expires"])
                 .filter(auction["expires"] < self.CURRENT_TIME)
                 # > 1 by's not required by semantics of query, as done in noir nexmark test
                 .group_by([auction["id"], auction["category"]])
                 .aggregate(final_p=bid["price"].max())
                 # .join(auction, auction["category"] == auction["id"])
                 .group_by(auction["category"])
                 .aggregate(avg_final_p=_.final_p.mean()))

        compile_ibis_to_noir([(self.files["auction"], auction), (self.files["bid"], bid)],
                            query, run_after_gen=True, render_query_graph=True)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()
