import filecmp
import unittest
from codegen import generate
from codegen import ROOT_DIR
from ibis.expr.types.relations import Table
from ibis import _


class TestOperators(unittest.TestCase):

    def test_filter_select(self):
        def q_filter_select(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.string1 == "unduetre")
                    .select("int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_select, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-select.rs",
                        shallow=False))

    def test_filter_filter_select(self):
        def q_filter_filter_select(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.int1 == 123)
                    .filter(table.string1 == "unduetre")
                    # .select("int1", "string1")    # double select is unsupported: select does Cols -> tuple so no
                    # way for second select to decide number to go in .map(|x| x.num) based on col name
                    # possible fix: create struct with same field names as table for each select encountered
                    .select("string1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_filter_select, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-filter-select.rs",
                        shallow=False))

    def test_filter_group_select(self):
        def q_filter_group_select(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.string1 == "unduetre")
                    .group_by("string1").aggregate()
                    .select("string1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_group_select, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-select.rs",
                        shallow=False))

    def test_filter_group_mutate(self):
        def q_filter_group_mutate(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.string1 == "unduetre")
                    .group_by("string1").aggregate()
                    .mutate(
                int1=table.int1 * 20))  # mutate always results in alias preceded by Multiply (or other bin op)

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_group_mutate, run_after_gen=True, render_query_graph=True)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-mutate.rs",
                        shallow=False))

    def test_filter_group_mutate_reduce(self):
        def q_filter_group_mutate_reduce(tables: list[Table]) -> Table:
            table = tables[0]
            return (table
                    .filter(table.string1 == "unduetre")
                    .group_by("string1").aggregate(int1_agg=table["int1"].first())  # not adding an aggregation function loses all
                                                                # columns aside from selected ones
                    .mutate(mul=_.int1_agg * 20)    # keeping same name for new column messes up original column
                    # required to use `_` operator to refer to column of table being processed (doesn't exist in
                    # original table)
                    .aggregate(by=["string1"], max=_.mul.max()))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv"]
        generate(table_files, q_filter_group_mutate_reduce, run_after_gen=True, render_query_graph=True)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-mutate"
                                                                            "-reduce.rs",
                        shallow=False))

    def test_inner_join(self):
        def q_inner_join(tables: list[Table]) -> Table:
            return (tables[0]
                    .join(tables[1], "int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        generate(table_files, q_inner_join, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/inner-join.rs",
                                    shallow=False))

    def test_outer_join(self):
        def q_outer_join(tables: list[Table]) -> Table:
            return (tables[0]
                    .outer_join(tables[1], "int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        generate(table_files, q_outer_join, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/outer-join.rs",
                                    shallow=False))

    def test_left_join(self):
        def q_left_join(tables: list[Table]) -> Table:
            return (tables[0]
                    .left_join(tables[1], "int1"))

        table_files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        generate(table_files, q_left_join, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/left-join.rs",
                                    shallow=False))


if __name__ == '__main__':
    unittest.main()
