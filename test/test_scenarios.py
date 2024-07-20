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
        self.query = (self.tables["ints_strings"]
                      .filter((_.int1 % 2 == 0) & _.int4.notnull())
                      .select("int1"))

        self.complete_test_tasks("ints_strings")

    # def test_scenarios_preprocess_4_group_agg_sum(self):
    #     self.test_nullable_filter_group_select()
