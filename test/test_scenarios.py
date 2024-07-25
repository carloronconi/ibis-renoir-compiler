from test.test_operators import TestNullableOperators
from test.test_base import TestCompiler
from ibis import _
import ibis


class TestScenarios(TestNullableOperators):

    # preprocess

    def test_scenarios_preprocess_1_dropna(self):
        table = self.tables["ints_strings"]
        self.query = (table
                      .filter(table.int4.notnull()))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_preprocess_2_fillna(self):
        self.query = (self.tables["ints_strings"]
                      .int4.fillna(0))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_preprocess_3_filter(self):
        # filter with two conditions including one requiring computation
        self.query = (self.tables["ints_strings"]
                      .filter((_.int1 % 2 == 0) & _.int4.notnull())
                      .select("int1"))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_preprocess_4_group_sum(self):
        # filter out many, group and aggregate with one accumulator
        self.query = (self.tables["ints_strings"]
                      .filter(_.string1.contains("a"))
                      .group_by("string1")
                      .aggregate(int1_agg=_["int1"].sum())
                      .select(["int1_agg"]))
        self.complete_test_tasks("ints_strings")
        
    def test_scenarios_preprocess_5_group_mean(self):
        # without filtering, group and aggregate with two accumulators
        self.query = (self.tables["ints_strings"]
                      .group_by("string1")
                      .aggregate(int1_agg=_["int1"].mean())
                      .select(["int1_agg"]))
        self.complete_test_tasks("ints_strings")

    # analytics

    def test_scenarios_analytics_1_filter(self):
        # ints_strings is already pre-aggregated: group_by string1, aggregate int4.sum(), int1.max()
        # filter with complex condition on two columns
        self.query = (self.tables["ints_strings"]
                      .filter(((_.int1 - _.int4) % 3 > 0))
                      .select("int1", "int4"))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_analytics_2_group_mean(self):
        # group and aggregate on int1, which was pre-aggregated by previous step
        self.query = (self.tables["ints_strings"]
                      .group_by("int1")
                      .aggregate(int4_agg=_["int4"].mean())
                      .select(["int4_agg"]))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_analytics_3_inner_join(self):
        # join with many_ints on int1 filtered by int3 not null
        # un-optimizable filter based on field from either table
        table1 = self.tables["ints_strings"]
        table2 = self.tables["many_ints"]
        self.query = (table2
                      .filter(_.int3.notnull())
                      .join(table1, "int1")
                      .filter((_.int4 + _.int3) % 2 == 0)
                      .select(["int4", "int3"]))
        self.complete_test_tasks()

    def test_scenarios_analytics_4_outer_join(self):
        # outer join and un-optimizable mutate
        table1 = self.tables["ints_strings"]
        table2 = self.tables["many_ints"]
        self.query = (table2
                      .filter(_.int3 % 5 > 0)
                      .outer_join(table1, "int1")
                      .mutate(res=(_.int4 + _.int3) / 2)
                      .select(["res"]))
        self.complete_test_tasks()

    def test_scenarios_analytics_5_window(self):
        # outer join followed by window function computing mean
        # within the groups 
        table1 = self.tables["ints_strings"]
        table2 = self.tables["many_ints"]
        self.query = (table2
                      .outer_join(table1, "int1")
                      .group_by("int1")
                      .mutate(gr_i3_mean=_.int3.mean())
                      .select(["int1", "gr_i3_mean"]))
        self.complete_test_tasks()

    # exploration

    def test_scenarios_exploration_1_filter(self):
        # filter with complex condition on two columns
        self.query = (self.tables["ints_strings"]
                      .filter(((_.int1 - _.int4) % 3 > 0) & _.int4.notnull())
                      .select("int1", "int4"))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_exploration_2_mutate(self):
        # mutate and filter
        self.query = (self.tables["ints_strings"]
                      .mutate(res=(_.int1 - _.int4) * 3)
                      .filter(_.res % 2 == 0)
                      .select(["res"]))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_exploration_3_window_group(self):
        # compute group TSS (total sum of squares) for int1
        self.query = (self.tables["ints_strings"]
                      .group_by("string1")
                      .mutate(gr_i1_demean=_.int1 - _.int1.mean())
                      .group_by("string1")
                      .aggregate(gr_i1_sum_dem=_.gr_i1_demean.sum())
                      .mutate(gr_i1_TSS=_.gr_i1_sum_dem * _.gr_i1_sum_dem))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_exploration_4_group_mean(self):
        # simple group-by and aggregate mean with two accumulators
        self.query = (self.tables["ints_strings"]
                      .group_by("string1")
                      .aggregate(int4_agg=_["int4"].mean())
                      .select(["int4_agg"]))
        self.complete_test_tasks("ints_strings")

    def test_scenarios_exploration_5_inner_join(self):
        # filter both tables for not null values and join
        # with un-optimizable filter based on field from either table
        table1 = self.tables["ints_strings"]
        table2 = self.tables["many_ints"]
        self.query = (table2
                      .filter(_.int3.notnull())
                      .join(table1
                            .filter(_.int4.notnull()), "int1")
                      .filter((_.int4 + _.int3) % 2 == 0)
                      .select(["int4", "int3"]))
        self.complete_test_tasks()


class TestScenariosViews(TestCompiler):
    def setUp(self):
        self.init_files()
        self.init_tables()
        super().setUp()

    def init_files(self, file_suffix=""):
        # no files are required for this test, so we only initialize the name
        # of the single source
        self.files = {"source_kafka": None,}
        return
    
    def init_tables(self):
        # create the sources reading from the kafka topic
        source_schema = ibis.schema({"createTime": ibis.dtype("string"),
                                     "orderId": ibis.dtype("int64"),
                                     "category": ibis.dtype("string"),
                                     "merchantId": ibis.dtype("int64")})
        source_name = next(iter(self.files))
        con = ibis.get_backend()

        if con.name == "pyspark":
            table: ibis.Table = con.read_kafka(
                       table_name=source_name,
                       auto_parse=True,
                       schema=source_schema,
                       options={"kafka.bootstrap.servers": "localhost:9092", 
                                "subscribe": "source",
                                "startingOffsets": "latest",
                                "failOnDataLoss": "false"})
        elif con.name == "risingwave":
            table: ibis.Table = con.create_source(
                name=source_name,
                schema=source_schema,
                connector_properties={"connector": "kafka",
                                      "topic": "source",
                                      "properties.bootstrap.server": "localhost:9092",
                                      "scan.startup.mode": "latest",
                                      "scan.startup.timestamp.millis": "140000000"},
                data_format="PLAIN",
                encode_format="JSON"
            )
        else:
            raise NotImplementedError(f"Backend {con.name} not supported for views test")
        self.tables = {source_name: table}

    
    def test_scenarios_views_1_filter(self):
        self.query = (self.tables["source_kafka"]
                      .filter(_.merchantId % 2 == 0)
                      # TODO: `value` field always required, add it in the data generator and schema
                      .mutate(value=_.category)
                      .select(["merchantId", "value"]))
        # no complete_test_tasks here, as renoir is not supported