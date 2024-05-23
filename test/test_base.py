import os
import sys
import unittest
import pandas as pd
from difflib import unified_diff
from codegen import ROOT_DIR
from ibis import _


class TestCompiler(unittest.TestCase):
    run_after_gen = True
    render_query_graph = False

    def setUp(self):
        try:
            os.remove(ROOT_DIR + "/out/noir-result.csv")
        except FileNotFoundError:
            pass

    def assert_equality_noir_source(self):
        test_expected_file = "/test/expected/" + \
            sys._getframe().f_back.f_code.co_name + ".rs"

        with open(ROOT_DIR + test_expected_file, "r") as f:
            expected_lines = f.readlines()
        with open(ROOT_DIR + "/noir-template/src/main.rs", "r") as f:
            actual_lines = f.readlines()

        diff = list(unified_diff(expected_lines, actual_lines))
        self.assertEqual(diff, [], "Differences:\n" + "".join(diff))
        print("\033[92m Source equality: OK\033[00m")

    def assert_similarity_noir_output(self, query, noir_subset_ibis=False):
        print(query.head(50).to_pandas())
        df_ibis = query.to_pandas()
        self.round_float_cols(df_ibis)
        df_ibis.to_csv(ROOT_DIR + "/out/ibis-result.csv")

        noir_path = ROOT_DIR + "/out/noir-result.csv"
        # if noir file has size 0 it means no output rows were generated by the query and read_csv will fail
        # this happens because noir doesn't output the header row when output has 0 rows (while ibis does), so we need to
        # consider df_noir as an empty dataframe, and just check that df_ibis is empty
        if os.path.getsize(noir_path) == 0:
            self.assertEqual(len(df_ibis.index), 0,
                             "Noir output is 0 rows, while ibis is not!")
            return

        df_noir = pd.read_csv(noir_path)
        self.round_float_cols(df_noir)

        # with keyed streams, noir preserves the key column with its original name
        # with joins, both the key column and the corresponding cols in joined tables are preserved
        # with outer joins, the left preserved col could have NaNs that the key doesn't have, so drop the key col and
        # preserve left joined col instead
        noir_cols = list(df_noir.columns)
        if len(noir_cols) > 1 and noir_cols[1] == noir_cols[0] + ".1":
            df_noir.drop(noir_cols[0], axis=1, inplace=True)
            df_noir.rename(columns={noir_cols[1]: noir_cols[0]}, inplace=True)

        # noir can output duplicate columns and additional columns, so remove duplicates and select those in ibis output
        df_noir = df_noir.loc[:, ~df_noir.columns.duplicated(
        )][df_ibis.columns.tolist()]

        # dataframes now should be exactly the same aside from row ordering:
        # group by all columns and count occurrences of each row
        df_ibis = df_ibis.groupby(df_ibis.columns.tolist(
        ), dropna=False).size().reset_index(name="count")
        df_noir = df_noir.groupby(df_noir.columns.tolist(
        ), dropna=False).size().reset_index(name="count")

        # fast fail if occurrence counts have different lengths
        if not noir_subset_ibis:
            self.assertEqual(len(df_ibis.index), len(df_noir.index),
                             f"Row occurrence count tables must have same length! Got this instead:\n{df_ibis}\n{df_noir}")

        # occurrence count rows could still be in different order so use a join on all columns
        join = pd.merge(df_ibis, df_noir, how="outer",
                        on=df_ibis.columns.tolist(), indicator=True)
        both_count = join["_merge"].value_counts()["both"]
        join.to_csv(ROOT_DIR + "/out/ibis-noir-comparison.csv")

        if not noir_subset_ibis:
            self.assertEqual(both_count, len(join.index),
                             f"Row occurrence count tables must have same values! Got this instead:\n{join}")
        else:
            # here we allow for noir to output fewer rows than ibis
            # used for windowing, where ibis semantics don't include windows with size
            # smaller than specified, while noir does
            left_count = join["_merge"].value_counts()["left_only"]
            right_count = join["_merge"].value_counts()["right_only"]
            message = f"Noir output must be a subset of ibis output! Got this instead:\n{join}"
            self.assertGreaterEqual(left_count, 0, message)
            # only reason why a right_only row should exist is if an identical left_only row with a higher count exists
            if right_count > 0:
                cols = join.columns.tolist()
                cols.remove("count")
                cols.remove("_merge")
                for _, r_row in join[join["_merge"] == "right_only"].iterrows():
                    found = False
                    for _, l_row in join[join["_merge"] == "left_only"].iterrows():
                        if all(r_row[col] == l_row[col] for col in cols) and r_row["count"] < l_row["count"]:
                            found = True
                            break
                    self.assertTrue(found, message)
            else:
                self.assertEqual(right_count, 0, message)
            self.assertGreaterEqual(both_count, 0, message)

        print(f"\033[92m Output similarity: OK\033[00m")

    @staticmethod
    def round_float_cols(df: pd.DataFrame, decimals=3):
        for i, t in enumerate(df.dtypes):
            if t == "float64":
                df.iloc[:, i] = df.iloc[:, i].round(decimals)
