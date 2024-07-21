from test.test_operators import TestNullableOperators
from ibis import _


class TestScenarios(TestNullableOperators):

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

    # TODO: evcxr compile error
    # def test_scenarios_analytics_5_window(self):
    #     # outer join followed by window function aggregating 
    #     table1 = self.tables["ints_strings"]
    #     table2 = self.tables["many_ints"]
    #     self.query = (table2
    #                   .outer_join(table1, "int1")
    #                   .group_by("int1")
    #                   .mutate(gr_i4_demean=_.int4 - _.int4.mean(), gr_i3_mean=_.int3.mean())
    #                   .select(["gr_i4_demean", "gr_i3_mean"]))
    #     self.complete_test_tasks()