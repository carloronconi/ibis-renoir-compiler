from test.test_operators import TestNullableOperators
from codegen import compile_ibis_to_noir
from ibis import _

class TestScenarios(TestNullableOperators):
    
    def test_scenarios_preprocess_1_dropna(self):
        table = self.tables["ints_strings"]
        self.query = (table
                      .filter(table.int4.notnull()))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, True, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_scenarios_preprocess_2_fillna(self):
        table = self.tables["ints_strings"]
        self.query = (table
                      .int4.fillna(0))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, True, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()