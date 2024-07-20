from test.test_operators import TestNullableOperators
from codegen import compile_ibis_to_noir
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