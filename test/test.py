import filecmp
import unittest

import ibis

from codegen import compile_ibis_to_noir
from codegen import ROOT_DIR
from ibis import _


class TestOperators(unittest.TestCase):

    def test_filter_select(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.string1 == "unduetre")
                 .select("int1"))

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-select.rs",
                        shallow=False))

    def test_filter_filter_select(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.int1 == 123)
                 .filter(table.string1 == "unduetre")
                 # .select("int1", "string1")    # double select is unsupported: select does Cols -> tuple so no
                 # way for second select to decide number to go in .map(|x| x.num) based on col name
                 # possible fix: create struct with same field names as table for each select encountered
                 .select("string1"))

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-filter-select.rs",
                        shallow=False))

    def test_filter_group_select(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.string1 == "unduetre")
                 .group_by("string1")
                 .aggregate(int1_agg=table["int1"].first())
                 .select(["string1", "int1_agg"]))
        # TODO: test fails because ibis renames col when aggregating, do same in generated noir

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-select.rs",
                        shallow=False))

    def test_filter_group_mutate(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.string1 == "unduetre")
                 .group_by("string1")
                 .aggregate(int1_agg=table["int1"].first())
                 .mutate(mul=_.int1_agg * 20))  # mutate always results in alias preceded by Multiply (or other bin op)
        # TODO: same as before + mutate should create new col as in ibis instead of substituting

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-mutate.rs",
                        shallow=False))

    def test_filter_reduce(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.string1 == "unduetre")
                 .aggregate(int1_agg=table["int1"].first()))
        # here example of reduce without group_by

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-reduce.rs",
                        shallow=False))

    def test_filter_group_mutate_reduce(self):
        file = ROOT_DIR + "/data/int-1-string-1.csv"
        table = ibis.read_csv(file)
        query = (table
                 .filter(table.int1 > 200)
                 .mutate(mul=_.int1 * 20)
                 .group_by("string1")
                 # it makes no sense to mutate after group_by: as if didn't group_by! mutate before it
                 .aggregate(agg=_.int1.sum()))
        # TODO: after every map use new struct, otherwise following ops can't find col name

        # Solution (works because of two blocks below):
        # 1. encounter aggregate
        # 2. if has TableColumn below it's a group_by().reduce()
        # 3. otherwise it's just a reduce()

        # Not performing aggregation right after group by will ignore the group by!
        # .group_by("string1")
        # .mutate(mul=_.int1 * 20)
        # .aggregate(agg=_.mul.sum()))

        # Only ibis use case with group by not followed by aggregate
        # Still, it performs an almost-aggregation right after
        # For now not supporting this type of operator (can be expressed with
        # normal group by + reduce)
        # .group_by("string1")
        # .aggregate(int1_agg=table["int1"].first())
        # .mutate(center=_.int1 - _.int1.mean()))

        compile_ibis_to_noir([(file, table)], query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(
            filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/filter-group-mutate"
                                                                            "-reduce.rs",
                        shallow=False))

    def test_inner_join(self):
        files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        tables = [ibis.read_csv(file) for file in files]
        query = (tables[0]
                 .join(tables[1], "int1"))

        compile_ibis_to_noir(zip(files, tables), query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/inner-join.rs",
                                    shallow=False))

    def test_outer_join(self):
        files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        tables = [ibis.read_csv(file) for file in files]
        query = (tables[0]
                 .outer_join(tables[1], "int1"))

        compile_ibis_to_noir(zip(files, tables), query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/inner-join.rs",
                                    shallow=False))

    def test_left_join(self):
        files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        tables = [ibis.read_csv(file) for file in files]
        query = (tables[0]
                 .left_join(tables[1], "int1"))

        compile_ibis_to_noir(zip(files, tables), query, run_after_gen=False, render_query_graph=False)

        self.assertTrue(filecmp.cmp(ROOT_DIR + "/noir-template/src/main.rs", ROOT_DIR + "/test/expected/inner-join.rs",
                                    shallow=False))


if __name__ == '__main__':
    unittest.main()