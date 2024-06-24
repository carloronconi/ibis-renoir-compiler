import ibis
from codegen.generator import compile_ibis_to_noir
from test.test_base import TestCompiler
from codegen import ROOT_DIR
from ibis import _


class TestNexmark(TestCompiler):
    CURRENT_TIME = 2330277279926

    def setUp(self):
        self.init_files()
        self.init_tables()
        super().setUp()

    def init_files(self, file_suffix=""):
        names = ["auction", "bid", "person"]
        file_prefix = ROOT_DIR + "/data/nexmark/"
        file_suffix = file_suffix + ".csv"
        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}

    def init_table_files(self, file_suffix=""):
        if ibis.get_backend().name == "flink":
            schemas = {"auction":   ibis.schema({"id": ibis.dtype("int64"),
                                                 "item_name": ibis.dtype("string"),
                                                 "description": ibis.dtype("string"),
                                                 "initial_bid": ibis.dtype("int64"),
                                                 "reserve": ibis.dtype("int64"),
                                                 "date_time": ibis.dtype("int64"),
                                                 "expires": ibis.dtype("int64"),
                                                 "seller": ibis.dtype("int64"),
                                                 "category": ibis.dtype("int64"),
                                                 "extra": ibis.dtype("string")}),
                       "bid":       ibis.schema({"auction": ibis.dtype("int64"),
                                                 "bidder": ibis.dtype("int64"),
                                                 "price": ibis.dtype("int64"),
                                                 "channel": ibis.dtype("string"),
                                                 "url": ibis.dtype("string"),
                                                 "date_time": ibis.dtype("int64"),
                                                 "extra": ibis.dtype("string")}),
                       "person":    ibis.schema({"id": ibis.dtype("int64"),
                                                 "name": ibis.dtype("string"),
                                                 "email_address": ibis.dtype("string"),
                                                 "credit_card": ibis.dtype("string"),
                                                 "city": ibis.dtype("string"),
                                                 "state": ibis.dtype("string"),
                                                 "date_time": ibis.dtype("int64"),
                                                 "extra": ibis.dtype("string")})}
            self.tables = {n: ibis.read_csv(
                f, schema=schemas[n]) for n, f in self.files.items()}
        else:
            self.tables = {n: ibis.read_csv(f) for n, f in self.files.items()}

    def test_nexmark_query_1(self):
        """
        SELECT Istream(auction, DOLTOEUR(price), bidder, datetime)
        FROM bid [ROWS UNBOUNDED];
        """

        bid = self.tables["bid"]
        self.query = (bid
                      .mutate(dol_price=bid["price"] * 0.85)
                      .select(["auction", "price", "dol_price", "bidder", "date_time"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["bid"], bid)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nexmark_query_2(self):
        """
        SELECT Rstream(auction, price)
        FROM Bid [NOW]
        WHERE auction = 1007 OR auction = 1020 OR auction = 2001 OR auction = 2019 OR auction = 2087;
        """

        bid = self.tables["bid"]
        self.query = (bid
                      .filter((bid["auction"] == 1007) | (bid["auction"] == 1020) | (bid["auction"] == 2001) | (bid["auction"] == 2019) | (bid["auction"] == 2087))
                      .select(["auction", "price"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["bid"], bid)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nexmark_query_3(self):
        """
        SELECT Istream(P.name, P.city, P.state, A.id)
        FROM Auction A [ROWS UNBOUNDED], Person P [ROWS UNBOUNDED]
        WHERE A.seller = P.id AND (P.state = `OR' OR P.state = `ID' OR P.state = `CA') AND A.category = 10;
        """

        auction = self.tables["auction"]
        person = self.tables["person"]
        self.query = (auction
                      .join(person, auction["seller"] == person["id"])
                      .filter((person["state"] == "OR") | (person["state"] == "ID") | (person["state"] == "CA"))
                      .filter(auction["category"] == 10)
                      .select(["name", "city", "state", "id"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["auction"], auction), (self.files["person"], person)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
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
        self.query = (auction
                      # TODO: writing equality in opposite order breaks noir
                      .join(bid, bid["auction"] == auction["id"])

                      # careful for ibis: using initial tab name w/ square brackets breaks col resolution for bid.date_time if bid is on right
                      # side of join, because its column (also present in auction) is renamed to date_time_right
                      # and would need to use _.date_time_right instead of bid["date_time"]
                      # if doing join in the opposite direction otherwise, it would still work
                      .filter((_.date_time_right < _.expires) & (_.expires < self.CURRENT_TIME))

                      # > 1 by's not required by semantics of self.query, as done in noir nexmark test
                      # but required to be able to group by category later (to have category col available)
                      # note for ibis: better use "_." when choosing columns, because using old table name with
                      # square brackets instead, can trigger implicit re-join with that table instead of selecting
                      # from intermediate result
                      .group_by([_.id, _.category])
                      .aggregate(final_p=_.price.max())
                      # nexmark self.query seems to suggest that there's a Category table to join, but actually
                      # it doesn't exist and the category is just a column in Auction table
                      .group_by(_.category)
                      .aggregate(avg_final_p=_.final_p.mean()))

        # print(ibis.to_sql(query))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["auction"], auction), (self.files["bid"], bid)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nexmark_query_6(self):
        """
        SELECT Istream(AVG(Q.final), Q.seller)
        FROM (SELECT Rstream(MAX(B.price) AS final, A.seller)
              FROM Auction A [ROWS UNBOUNDED], Bid B [ROWS UNBOUNDED]
              WHERE A.id=B.auction AND B.datetime < A.expires AND A.expires < CURRENT_TIME
              GROUP BY A.id, A.seller) [PARTITION BY A.seller ROWS 10] Q
        GROUP BY Q.seller;
        """

        auction = self.tables["auction"]
        bid = self.tables["bid"]
        w = ibis.window(group_by=[_.seller], preceding=9, following=0)
        self.query = (auction
                      .join(bid, bid["auction"] == auction["id"])
                      .filter((_.date_time_right < _.expires) & (_.expires < self.CURRENT_TIME))
                      .group_by([_.id, _.seller])
                      .aggregate(final_p=_.price.max())
                      .mutate(avg_final_p=_.final_p.mean().over(w)))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["auction"], auction), (self.files["bid"], bid)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        # subset option is not enough in this case: testing excluding the last .mutate confirms that outputs
        # are the same up to there, but the last .mutate uses a window over 10 close rows within the same seller group:
        # the ordering of rows is non-deterministic in ibis (and maybe noir too), so the avg_final_p will be different
        if self.perform_assertions:
            # self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    # Query 5, 7, 8 are not supported by ibis because they require time-based windowing
