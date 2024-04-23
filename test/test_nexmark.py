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
                 # TODO: writing equality in opposite order breaks noir
                 .join(bid, bid["auction"] == auction["id"])

                 # careful for ibis: using initial tab name w/ square brackets breaks col resolution for bid.date_time if bid is on right
                 # side of join, because its column (also present in auction) is renamed to date_time_right
                 # and would need to use _.date_time_right instead of bid["date_time"]
                 # if doing join in the opposite direction otherwise, it would still work
                 .filter((_.date_time_right < _.expires) & (_.expires < self.CURRENT_TIME))

                 # > 1 by's not required by semantics of query, as done in noir nexmark test
                 # but required to be able to group by category later (to have category col available)
                 # note for ibis: better use "_." when choosing columns, because using old table name with
                 # square brackets instead, can trigger implicit re-join with that table instead of selecting
                 # from intermediate result
                 .group_by([_.id, _.category])
                 .aggregate(final_p=_.price.max())
                 # nexmark query seems to suggest that there's a Category table to join, but actually
                 # it doesn't exist and the category is just a column in Auction table
                 .group_by(_.category)
                 .aggregate(avg_final_p=_.final_p.mean()))

        print(ibis.to_sql(query))
        """
        WITH t0 AS (
          SELECT
            t2.auction AS auction,
            t2.bidder AS bidder,
            t2.price AS price,
            t2.channel AS channel,
            t2.url AS url,
            t2.date_time AS date_time,
            t2.extra AS extra,
            t3.id AS id,
            t3.item_name AS item_name,
            t3.description AS description,
            t3.initial_bid AS initial_bid,
            t3.reserve AS reserve,
            t3.date_time AS date_time_right,
            t3.expires AS expires,
            t3.seller AS seller,
            t3.category AS category,
            t3.extra AS extra_right
          FROM main.ibis_read_csv_jqgzehe2rjhibipdymlgjcyrpu AS t2
          JOIN main.ibis_read_csv_azt4jc6etfbo7mtrbc5nxvhhrq AS t3
            ON t3.id = t2.auction
          WHERE
            t2.date_time < t3.expires AND t3.expires < CAST(2330277279926 AS BIGINT)
        )
        SELECT
          t1.category,
          AVG(t1.final_p) AS avg_final_p
        FROM (
          SELECT
            t0.id AS id,
            t0.category AS category,
            MAX(t0.price) AS final_p
          FROM t0
          GROUP BY
            1,
            2
        ) AS t1
        GROUP BY
          1
        """

        compile_ibis_to_noir([(self.files["auction"], auction), (self.files["bid"], bid)],
                             query, run_after_gen=True, render_query_graph=True)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()
