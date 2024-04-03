import os
import sys
import unittest

import ibis
import pandas as pd

from codegen import compile_ibis_to_noir
from codegen import ROOT_DIR
from ibis import _
from difflib import unified_diff


class TestOperators(unittest.TestCase):

    def setUp(self):
        self.files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        self.tables = [ibis.read_csv(file) for file in self.files]

        try:
            os.remove(ROOT_DIR + "/out/noir-result.csv")
        except FileNotFoundError:
            pass

    def test_filter_select(self):
        query = (self.tables[0]
                 .filter(_.string1 == "unduetre")
                 .select("int1"))

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_filter_filter_select_select(self):
        query = (self.tables[0]
                 .filter(_.int1 == 123)
                 .filter(_.string1 == "unduetre")
                 .select("int1", "string1")
                 .select("string1"))

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_filter_group_select(self):
        query = (self.tables[0]
                 .filter(_.string1 == "unduetre")
                 .group_by("string1")
                 .aggregate(int1_agg=_["int1"].first())
                 .select(["int1_agg"]))

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_filter_group_mutate(self):
        query = (self.tables[0]
                 .filter(_.string1 == "unduetre")
                 .group_by("string1")
                 .aggregate(int1_agg=_["int1"].first())
                 .mutate(mul=_.int1_agg * 20))  # mutate always results in alias preceded by Multiply (or other bin op)

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_filter_reduce(self):
        query = (self.tables[0]
                 .filter(_.string1 == "unduetre")
                 .aggregate(int1_agg=_["int1"].sum()))
        # here example of reduce without group_by

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_filter_group_mutate_reduce(self):
        query = (self.tables[0]
                 .filter(_.int1 > 200)
                 .mutate(mul=_.int1 * 20)
                 .group_by("string1")
                 # it makes no sense to mutate after group_by: as if didn't group_by! mutate before it
                 .aggregate(agg=_.mul.sum()))

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

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_inner_join_select(self):
        query = (self.tables[0]
                 .filter(_.int1 < 200)
                 .mutate(mul=_.int1 * 20)
                 .join(self.tables[1]
                       .mutate(sum=_.int3 + 100), "int1")
                 .select(["string1", "int1", "int3"]))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_outer_join(self):
        query = (self.tables[0]
                 .outer_join(self.tables[1], "int1"))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        # TODO: using defaults in noir is not semantically correct for query
        # TODO: using defaults in noir printed csv (empty strings specifically) breaks similarity check
        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_left_join(self):
        query = (self.tables[0]
                 .left_join(self.tables[1], "int1"))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_group_reduce_join_mutate(self):
        """
        Tests two cases:
        - mutate (could also be select) after join (which produces a KeyedStream of a tuple of joined structs)
        - group-reduce KeyedStream join with Stream (KeyedStream wants to join with another KeyedStream)
        """
        query = (self.tables[1]
                 .group_by("int1")
                 .aggregate(agg2=_.int2.sum())
                 .inner_join(self.tables[0], "int1")
                 .mutate(mut4=_.int4 + 100))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_group_reduce_group_reduce_join(self):
        """
        Tests joining KeyedStream with other var which is KeyedStream already
        """
        query = (self.tables[1]
                 .group_by("int1")
                 .aggregate(agg2=_.int2.sum())
                 .inner_join(self.tables[0]
                             .group_by("int1").aggregate(agg4=_.int4.sum()), "int1"))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def assert_equality_noir_source(self):
        test_expected_file = "/test/expected/" + sys._getframe().f_back.f_code.co_name + ".rs"

        with open(ROOT_DIR + test_expected_file, "r") as f:
            expected_lines = f.readlines()
        with open(ROOT_DIR + "/noir-template/src/main.rs", "r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        self.assertEqual(diff, [], "Differences:\n" + "".join(diff))
        print("\033[92m Source equality: OK\033[00m")

    def assert_similarity_noir_output(self, query):
        print(query.head(50).to_pandas())
        df_ibis = query.to_pandas()

        df_noir = pd.read_csv(ROOT_DIR + "/out/noir-result.csv", header=None)

        equal_cols = 0
        equal_col_names = []
        for col_ibis_name in df_ibis.columns:
            for col_noir_name in df_noir.columns:
                col_ibis = sorted(df_ibis[col_ibis_name].to_list())
                col_noir = sorted(df_noir[col_noir_name].to_list())
                if col_noir == col_ibis:
                    equal_col_names.append(col_noir_name)
                    df_noir.drop(col_noir_name, axis=1, inplace=True)
                    equal_cols += 1
                    break

        # check if all ibis columns are present in noir columns (with elements in any order)
        self.assertTrue(equal_cols == len(df_ibis.columns))

        df_noir = pd.read_csv(ROOT_DIR + "/out/noir-result.csv", header=None)
        for col in df_noir.columns:
            if col not in equal_col_names:
                df_noir.drop(col, axis=1, inplace=True)

        for i, row_ibis in df_ibis.iterrows():
            row_ibis = set(row_ibis.to_list())
            for n, row_noir in df_noir.iterrows():
                row_noir = set(row_noir.to_list())
                if row_noir == row_ibis:
                    df_noir.drop(n, axis="index", inplace=True)
                    break

        # check if each ibis row contain same set of values as one noir row (set due to strings not being sortable with ints)
        self.assertTrue(len(df_noir.index) == 0)
        print(f"\033[92m Output similarity: OK\033[00m")


if __name__ == '__main__':
    unittest.main()
