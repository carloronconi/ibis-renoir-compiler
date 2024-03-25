import filecmp
import unittest
from codegen import generate
from codegen import ROOT_DIR
from ibis.expr.types.relations import Table


class TestOperators(unittest.TestCase):

    def test_filter_select(self):
        def q_filter_select(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.string1 == "unduetre")
                    .select("int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_select)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-select.rs",
                        shallow=False))

    def test_inner_join(self):
        def q_inner_join(tables: list[Table]) -> Table:
            return (tables[0]
                    .join(tables[1], "int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        generate(table_files, q_inner_join)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/inner-join.rs",
                                    shallow=False))


if __name__ == '__main__':
    unittest.main()
