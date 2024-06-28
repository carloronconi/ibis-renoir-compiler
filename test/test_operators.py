import ibis
import unittest
import pandas as pd
from ibis import _

from codegen import ROOT_DIR
from codegen import compile_ibis_to_noir
from test.test_base import TestCompiler


class TestNullableOperators(TestCompiler):

    def setUp(self):
        self.init_files()
        self.init_tables()
        super().setUp()

    def init_files(self, file_suffix=""):
        names = ["ints_strings", "many_ints"]
        file_prefix = ROOT_DIR + "/data/nullable_op/"
        file_suffix = file_suffix + ".csv"
        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}

    def init_tables(self):
        if ibis.get_backend().name == "flink":
            # flink requires to explicitly specify schema and doesn't support headers
            # we take care of headers when running benchmark, so that when we run the query
            # and it's loaded from file the same issue doesn't occur
            self.schemas = {"ints_strings":  ibis.schema({"int1": ibis.dtype("int64"),
                                                     "string1": ibis.dtype("string"),
                                                     "int4": ibis.dtype("int64")}),
                       "many_ints":     ibis.schema({"int1": ibis.dtype("int64"),
                                                     "int2": ibis.dtype("int64"),
                                                     "int3": ibis.dtype("int64")})}
            no_header_files = self.create_files_no_headers()
            self.tables = {n: ibis.read_csv(
                f, schema=self.schemas[n]) for n, f in no_header_files.items()}
        else:
            self.tables = {n: ibis.read_csv(f) for n, f in self.files.items()}

    def test_nullable_filter_select(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.string1 == "unduetre")
                      .select("int1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_filter_filter_select_select(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.int1 == 123)
                      .filter(_.string1 == "unduetre")
                      .select("int1", "string1")
                      .select("string1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_filter_group_select(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.string1 == "unduetre")
                      .group_by("string1")
                      .aggregate(int1_agg=_["int1"].sum())
                      .select(["int1_agg"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_filter_group_mutate(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.string1 == "unduetre")
                      .group_by("string1")
                      .aggregate(int1_agg=_["int1"].sum())
                      .mutate(mul=_.int1_agg * 20))  # mutate always results in alias preceded by Multiply (or other bin op)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_filter_reduce(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.string1 == "unduetre")
                      .aggregate(int1_agg=_["int1"].sum()))
        # here example of reduce without group_by

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_filter_group_mutate_reduce(self):
        self.query = (self.tables["ints_strings"]
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

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["ints_strings"], self.tables["ints_strings"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_inner_join_select(self):
        self.query = (self.tables["ints_strings"]
                      .filter(_.int1 < 200)
                      .mutate(mul=_.int1 * 20)
                      .join(self.tables["many_ints"]
                            .mutate(sum=_.int3 + 100), "int1")
                      .select(["string1", "int1", "int3"]))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_outer_join(self):
        self.query = (self.tables["ints_strings"]
                      .outer_join(self.tables["many_ints"], "int1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_left_join(self):
        self.query = (self.tables["ints_strings"]
                      .left_join(self.tables["many_ints"], "int1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_group_reduce_join_mutate(self):
        """
        Tests two cases:
        - mutate (could also be select) after join (which produces a KeyedStream of a tuple of joined structs)
        - group-reduce KeyedStream join with Stream (KeyedStream wants to join with another KeyedStream)
        """
        self.query = (self.tables["many_ints"]
                      .group_by("int1")
                      .aggregate(agg2=_.int2.sum())
                      .inner_join(self.tables["ints_strings"], "int1")
                      .mutate(mut4=_.int4 + 100))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_group_reduce_group_reduce_join(self):
        """
        Tests joining KeyedStream with other var which is KeyedStream already
        """
        self.query = (self.tables["many_ints"]
                      .group_by("int1")
                      .aggregate(agg2=_.int2.sum())
                      .inner_join(self.tables["ints_strings"]
                                  .group_by("int1").aggregate(agg4=_.int4.sum()), "int1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_join_group_reduce(self):
        """
        Tests joining left non-KeyedStream with right KeyedStream
        """
        self.query = (self.tables["many_ints"]
                      .inner_join(self.tables["ints_strings"]
                                  .group_by("int1")
                                  .aggregate(agg4=_.int4.sum()), "int1"))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_nullable_windowing_implicit_mean(self):
        # here implicit windowing takes all the rows in the table, because no group_by is performed before the mutate
        # and the window is not explicitly defined
        self.query = (self
                      .tables["ints_strings"]
                      .mutate(int4_demean=_.int4 - _.int4.mean(), int4_mean=_.int4.mean()))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    def test_nullable_windowing_implicit_sum(self):
        # here implicit windowing takes all the rows in the table, because no group_by is performed before the mutate
        # and the window is not explicitly defined
        self.query = (self
                      .tables["ints_strings"]
                      .mutate(int4_sum=_.int4.sum()))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    def test_nullable_windowing_implicit_group(self):
        # here windowing is implicit over the whole group that was grouped before the mutate aggregation
        # so group_mean is actually the mean of the whole group having same string1
        self.query = (self
                      .tables["ints_strings"]
                      .group_by("string1")
                      .mutate(int4_demean=_.int4 - _.int4.mean(), group_mean=_.int4.mean()))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    def test_nullable_windowing_explicit_group(self):
        # this window first groups by string1, then, keeping original ordering within groups, computes aggregation (mean)
        # over the current row, and the preceding 1 row (2 rows total)
        # if the group the preceding/following rows are finished the mean is computed over fewer rows
        #
        # noir semantics only support following=0
        # ibis with preceding 1 aggregates preceding and itself, so translated to step=2
        # semantic difference: ibis takes up to 1 preceding row and itself, for a total of 2, while noir takes exactly 2 so produces fewer result rows
        # i.e. ibis takes all windows with size 2 and below, while noir only takes windows with size 2
        w = ibis.window(group_by="string1", preceding=1, following=0)
        self.query = (self.tables["ints_strings"]
                      .mutate(group_percent=_.int4 * 100 / _.int4.sum().over(w), group_sum=_.int4.sum().over(w)))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    def test_nullable_windowing_explicit(self):
        # same as previous but without group_by
        # here we test mean aggregation function instead of sum
        w = ibis.window(preceding=1, following=0)
        self.query = (self.tables["ints_strings"]
                      .mutate(group_mean=_.int4.mean().over(w)))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()

    def test_nullable_windowing_explicit_window_far(self):
        # same as previous but testing complex aggregation function that
        # makes WindowFunction not direct __children__ of Alias but child of child
        # so for now not recognized as ExplicitWindowOperator
        w = ibis.window(preceding=1, following=0)
        self.query = (self.tables["ints_strings"]
                      .mutate(group_perc=_.int4 * 100 / _.int4.mean().over(w)))

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output(noir_subset_ibis=True)
            self.assert_equality_noir_source()


class TestNonNullableOperators(TestCompiler):

    def setUp(self):
        self.init_files()
        self.init_tables()
        super().setUp()

    def init_files(self, file_suffix=""):
        names = ["fruit_left", "fruit_right"]
        file_prefix = ROOT_DIR + "/data/non_nullable_op/"
        file_suffix = file_suffix + ".csv"
        self.files = {n: f"{file_prefix}{n}{file_suffix}" for n in names}

    def init_tables(self):
        self.schema = ibis.schema({"fruit": ibis.dtype("!string"),
                                   "weight": ibis.dtype("!int64"),
                                   "price": ibis.dtype("int64")})
        self.tables = {n: ibis.table(self.schema)
                       for n, _ in self.files.items()}

    def test_non_nullable_filter_select(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .filter(_.fruit == "Apple")
                                          .select("price"))
        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_filter_filter_select_select(self):
        self.query_func = lambda tables:  (tables["fruit_left"]
                                           .filter(_.price > 3)
                                           .filter(_.fruit == "Apple")
                                           .select("fruit", "weight")
                                           .select("fruit"))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_filter_group_select(self):
        self.query_func = lambda tables:  (tables["fruit_left"]
                                           .filter(_.fruit == "Orange")
                                           .group_by("fruit")
                                           .aggregate(int1_agg=_["price"].sum())
                                           .select(["int1_agg"]))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_filter_group_mutate(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .filter(_.fruit == "Orange")
                                          .group_by("fruit")
                                          .aggregate(int1_agg=_["price"].sum())
                                          .mutate(mul=_.int1_agg * 20))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_filter_reduce(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .filter(_.fruit == "Orange")
                                          .aggregate(int1_agg=_["weight"].sum()))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_filter_group_mutate_reduce(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .filter(_.weight > 4)
                                          .mutate(mul=_.price * 20)
                                          .group_by("fruit")
                                          .aggregate(agg=_.mul.sum()))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files["fruit_left"], self.tables["fruit_left"])],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_inner_join_select(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .filter(_.weight > 2)
                                          .mutate(mul=_.price + 10)
                                          .join(tables["fruit_right"]
                                                .mutate(sum=_.price + 100), "fruit")
                                          .select(["fruit", "weight", "price"]))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_left_join(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .left_join(tables["fruit_right"], "fruit"))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_outer_join(self):
        self.query_func = lambda tables: (tables["fruit_left"]
                                          .outer_join(tables["fruit_right"], "fruit"))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_group_reduce_join_mutate(self):
        self.query_func = lambda tables: (tables["fruit_right"]
                                          .group_by("fruit")
                                          .aggregate(agg2=_.weight.sum())
                                          .inner_join(tables["fruit_left"], "fruit")
                                          .mutate(mut4=_.price + 100))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_group_reduce_group_reduce_join(self):
        self.query_func = lambda tables: (tables["fruit_right"]
                                          .group_by("fruit")
                                          .aggregate(agg2=_.price.sum())
                                          .inner_join(tables["fruit_left"]
                                                      .group_by("fruit").aggregate(agg4=_.weight.sum()), "fruit"))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()

    def test_non_nullable_join_group_reduce(self):
        self.query_func = lambda tables: (tables["fruit_right"]
                                          .inner_join(tables["fruit_left"]
                                                      .group_by("fruit")
                                                      .aggregate(agg4=_.price.sum()), "fruit"))

        self.query = self.query_func(self.tables)

        if self.perform_compilation:
            compile_ibis_to_noir([(self.files[k], self.tables[k]) for k in self.files.keys()],
                                 self.query, self.run_after_gen, self.print_output_to_file, self.render_query_graph, self.benchmark)

        if self.perform_assertions:
            self.assert_similarity_noir_output()
            self.assert_equality_noir_source()
