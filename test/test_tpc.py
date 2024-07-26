import ibis
from codegen.generator import compile_ibis_to_noir
from test.test_base import TestCompiler
from codegen import ROOT_DIR
from ibis import _


class TestTpcH(TestCompiler):

    def setUp(self):
        self.init_files()
        self.init_tables()
        super().setUp()

    def init_files(self, file_suffix=""):
        names = ["customer", "lineitem", "nation", "orders",
                 "part", "partsupp", "region", "supplier"]
        file_prefix = ROOT_DIR + "/data/tpch/"
        # file_suffix parameter is actually ignored
        file_suffix = ".csv"
        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}
        self.tab_count = len(names)
        
    def init_tables(self):
        self.tables = {n: ibis.read_csv(f) for n, f in self.files.items()}

    def test_tpc_h_query_1(self):
        """
        select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
        from
                lineitem
        where
                l_shipdate <= date '1998-12-01' - interval ':1' day (3)
        group by
                l_returnflag,
                l_linestatus
        order by
                l_returnflag,
                l_linestatus;
        """

        lineitem = self.tables["lineitem"]
        self.query = (lineitem
                      # TODO: - interval ':1' day (3)
                      .filter(lineitem["shipdate"] <= "1998-12-01")
                      .group_by(["returnflag", "linestatus"])
                      .aggregate(
                          sum_qty=lineitem["quantity"].sum(),
                          sum_base_price=lineitem["extendedprice"].sum(),
                          sum_disc_price=(
                              lineitem["extendedprice"] * (1 - lineitem["discount"])).sum(),
                          sum_charge=(
                              lineitem["extendedprice"] * (1 - lineitem["discount"]) * (1 + lineitem["tax"])).sum(),
                          avg_qty=lineitem["quantity"].mean(),
                          avg_price=lineitem["extendedprice"].mean(),
                          avg_disc=lineitem["discount"].mean(),
                          count_order=lineitem.count())
                      .order_by(["returnflag", "linestatus"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["lineitem"], lineitem)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()
