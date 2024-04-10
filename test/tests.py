import os
import sys
import unittest
from difflib import unified_diff

import ibis
import pandas as pd
from ibis import _

from codegen import ROOT_DIR
from codegen import compile_ibis_to_noir


class TestCompiler(unittest.TestCase):

    def setUp(self):
        try:
            os.remove(ROOT_DIR + "/out/noir-result.csv")
        except FileNotFoundError:
            pass

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
        df_noir = pd.read_csv(ROOT_DIR + "/out/noir-result.csv")

        # noir can output duplicate columns and additional columns, so remove duplicates and select those in ibis output
        df_noir = df_noir.loc[:, ~df_noir.columns.duplicated()][df_ibis.columns.tolist()]

        # dataframes now should be exactly the same aside from row ordering:
        # group by all columns and count occurrences of each row
        df_ibis = df_ibis.groupby(df_ibis.columns.tolist(), dropna=False).size().reset_index(name="count")
        df_noir = df_noir.groupby(df_noir.columns.tolist(), dropna=False).size().reset_index(name="count")

        # fast fail if occurrence counts have different lengths
        self.assertEqual(len(df_ibis.index), len(df_noir.index),
                         f"Row occurrence count tables must have same length! Got this instead:\n{df_ibis}\n{df_noir}")

        # occurrence count rows could still be in different order so use a join on all columns
        join = pd.merge(df_ibis, df_noir, how="outer", on=df_ibis.columns.tolist(), indicator=True)
        both_count = join["_merge"].value_counts()["both"]

        self.assertEqual(both_count, len(join.index),
                         f"Row occurrence count tables must have same values! Got this instead:\n{join}")

        print(f"\033[92m Output similarity: OK\033[00m")


class TestOperators(TestCompiler):

    def setUp(self):
        self.files = [ROOT_DIR + "/data/int-1-string-1.csv", ROOT_DIR + "/data/int-3.csv"]
        self.tables = [ibis.read_csv(file) for file in self.files]

        super().setUp()

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


class TestNonNullableOperators(TestCompiler):

    def setUp(self):
        df_non_null_cols_left = pd.DataFrame({'fruit': ["Orange", "Apple", "Kiwi", "Cherry", "Banana", "Grape", "Orange", "Apple"],
                                         'weight': [2, 15, 3, 24, 5, 16, 2, 17], 'price': [7, 10, 3, 5, 6, 23, 8, 20]})
        df_non_null_cols_right = pd.DataFrame({'fruit': ["Orange", "Apple", "Kiwi", "Apple"],
                                         'weight': [5, 12, 7, 27], 'price': [5, 11, 2, 8]})

        file_left = ROOT_DIR + "/data/fruit_left.csv"
        file_right = ROOT_DIR + "/data/fruit_right.csv"
        df_non_null_cols_left.to_csv(file_left, index=False)
        df_non_null_cols_right.to_csv(file_right, index=False)

        # creating schema with datatypes from pandas allows to pass nullable=False
        schema = ibis.schema({"fruit": ibis.dtype("!string"),
                              "weight": ibis.dtype("!int64"),
                              "price": ibis.dtype("int64")})  # non-nullable types are preceded by "!"

        # memtable allows to pass schema explicitly
        tab_non_null_cols_left = ibis.memtable(df_non_null_cols_left, schema=schema)
        tab_non_null_cols_right = ibis.memtable(df_non_null_cols_right, schema=schema)

        self.files = [file_left, file_right]
        self.tables = [tab_non_null_cols_left, tab_non_null_cols_right]

        super().setUp()

    def test_non_nullable_cols_filter_group_mutate_reduce(self):
        query = (self.tables[0]
                 .filter(_.weight > 4)
                 .mutate(mul=_.price * 20)
                 .group_by("fruit")
                 .aggregate(agg=_.mul.sum()))

        compile_ibis_to_noir([(self.files[0], self.tables[0])], query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_non_nullable_cols_inner_join_select(self):
        query = (self.tables[0]
                 .filter(_.weight > 2)
                 .mutate(mul=_.price + 10)
                 .join(self.tables[1]
                       .mutate(sum=_.price + 100), "fruit")
                 .select(["fruit", "weight", "price"]))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_non_nullable_left_join(self):
        query = (self.tables[0]
                 .left_join(self.tables[1], "fruit"))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()

    def test_non_nullable_outer_join(self):
        query = (self.tables[0]
                 .outer_join(self.tables[1], "fruit"))

        compile_ibis_to_noir(zip(self.files, self.tables), query, run_after_gen=True, render_query_graph=False)

        self.assert_similarity_noir_output(query)
        self.assert_equality_noir_source()


if __name__ == '__main__':
    unittest.main()
