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
        file_suffix = file_suffix + ".csv"
        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}
        self.tab_count = len(names)
        
    def init_tables(self):
        backend = ibis.get_backend().name
        if backend == "flink":
            schemas = {"customer":   ibis.schema({"custkey": ibis.dtype("int64"),
                                                  "name": ibis.dtype("string"),
                                                  "address": ibis.dtype("string"),
                                                  "nationkey": ibis.dtype("int64"),
                                                  "phone": ibis.dtype("string"),
                                                  "acctbal": ibis.dtype("float64"),
                                                  "mktsegment": ibis.dtype("string"),
                                                  "comment": ibis.dtype("string")}),
                       "lineitem":  ibis.schema({"orderkey": ibis.dtype("int64"),
                                                 "partkey": ibis.dtype("int64"),
                                                 "suppkey": ibis.dtype("int64"),
                                                 "linenumber": ibis.dtype("int64"),
                                                 "quantity": ibis.dtype("float64"),
                                                 "extendedprice": ibis.dtype("float64"),
                                                 "discount": ibis.dtype("float64"),
                                                 "tax": ibis.dtype("float64"),
                                                 "returnflag": ibis.dtype("string"),
                                                 "linestatus": ibis.dtype("string"),
                                                 "shipdate": ibis.dtype("string"),
                                                 "commitdate": ibis.dtype("string"),
                                                 "receiptdate": ibis.dtype("string"),
                                                 "shipinstruct": ibis.dtype("string"),
                                                 "shipmode": ibis.dtype("string"),
                                                 "comment": ibis.dtype("string")}),
                       "nation":    ibis.schema({"nationkey": ibis.dtype("int64"),
                                                "name": ibis.dtype("string"),
                                                "regionkey": ibis.dtype("int64"),
                                                "comment": ibis.dtype("string")}),
                        "orders":    ibis.schema({"orderkey": ibis.dtype("int64"),
                                                  "custkey": ibis.dtype("int64"),
                                                  "orderstatus": ibis.dtype("string"),
                                                  "totalprice": ibis.dtype("float64"),
                                                  "orderdate": ibis.dtype("string"),
                                                  "orderpriority": ibis.dtype("string"),
                                                  "clerk": ibis.dtype("string"),
                                                  "shippriority": ibis.dtype("int64"),
                                                  "comment": ibis.dtype("string")}),
                        "part":      ibis.schema({"partkey": ibis.dtype("int64"),
                                                  "name": ibis.dtype("string"),
                                                  "mfgr": ibis.dtype("string"),
                                                  "brand": ibis.dtype("string"),
                                                  "type": ibis.dtype("string"),
                                                  "size": ibis.dtype("int64"),
                                                  "container": ibis.dtype("string"),
                                                  "retailprice": ibis.dtype("float64"),
                                                  "comment": ibis.dtype("string")}),
                        "partsupp":  ibis.schema({"partkey": ibis.dtype("int64"),
                                                  "suppkey": ibis.dtype("int64"),
                                                  "availqty": ibis.dtype("int64"),
                                                  "supplycost": ibis.dtype("float64"),
                                                  "comment": ibis.dtype("string")}),
                        "region":    ibis.schema({"regionkey": ibis.dtype("int64"),
                                                  "name": ibis.dtype("string"),
                                                  "comment": ibis.dtype("string")}),
                        "supplier":  ibis.schema({"suppkey": ibis.dtype("int64"),
                                                  "name": ibis.dtype("string"),
                                                  "address": ibis.dtype("string"),
                                                  "nationkey": ibis.dtype("int64"),
                                                  "phone": ibis.dtype("string"),
                                                  "acctbal": ibis.dtype("float64"),
                                                  "comment": ibis.dtype("string")})
                       }
            no_header_files = self.create_files_no_headers()
            self.tables = {n: ibis.read_csv(
                f, schema=schemas[n]) for n, f in no_header_files.items()}
        else:
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
                      # note: interval ':1' day translated by manually changing the date
                      .filter(lineitem["shipdate"] <= "1998-11-01")
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
                      #.order_by(["returnflag", "linestatus"])
                        # TODO: order_by operator
                        # using  collect_to_vec and then sorting by multiple columns the same way `ORDER BY` does,
                        # sorting with the second column only when the first one is equal for some rows. Idea:
                        #     dummies.sort_by(|d1, d2| {
                        #       let cmp_x = d1.x.cmp(&d2.x);
                        #       if cmp_x == std::cmp::Ordering::Equal {
                        #            d1.z.cmp(&d2.z)
                        #     } else {
                        #       cmp_x
                        #     }
                      )
        
        self.render_query_graph = True

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["lineitem"], lineitem)],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()
