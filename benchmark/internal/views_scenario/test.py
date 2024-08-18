import csv
from dataclasses import dataclass
from typing import Callable, Generator, Any, NoReturn
from ibis import Table
from ibis import _
import ibis
import random


@dataclass
class StreamTable:
    name: str
    schema: ibis.Schema
    generator: Callable[[], Generator[dict[str, Any], Any, NoReturn]]


class TestViewsCustom:
    streams = [
        StreamTable(
            "orders",
            ibis.schema({
                "order_id": ibis.dtype("string"),
                "product": ibis.dtype("string"),
                "price": ibis.dtype("int64"),
                "discount": ibis.dtype("float64"),
                "quantity": ibis.dtype("int64"),
                "customer_id": ibis.dtype("string"),
                # "date_time": ibis.dtype("timestamp")
            }),
            lambda: TestViewsCustom.orders_generator()
        ),
        StreamTable(
            "customers",
            ibis.schema({
                "customer_id": ibis.dtype("string"),
                "name": ibis.dtype("string"),
                "age": ibis.dtype("int64"),
                "country": ibis.dtype("string"),
                # "date_time": ibis.dtype("timestamp")
            }),
            lambda: TestViewsCustom.customers_generator()
        )]

    @staticmethod
    def orders_generator():
        products = {"book": 5, "shoes": 78, "hat": 12, "gloves": 9, "scarf": 32,
                    "glasses": 84, "watch": 143, "phone": 1199, "laptop": 1499, "tablet": 799}
        while True:
            product, price = random.choice(list(products.items()))
            yield {"order_id": f"order_{random.randint(1, 1000)}",
                   "product": product,
                   "price": price,
                   "discount": random.uniform(0, 0.5),
                   "quantity": random.randint(1, 100),
                   "customer_id": f"customer_{random.randint(1, 100)}"}

    @staticmethod
    def customers_generator():
        countries = ["USA", "UK", "Germany", "France", "Italy",
                     "Spain", "Japan", "China", "Russia", "Brazil"]
        names = ["John", "Alice", "Bob", "Charlie", "David",
                 "Eve", "Frank", "Grace", "Helen", "Ivy"]
        surnames = ["Smith", "Johnson", "Williams", "Jones",
                    "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor"]
        while True:
            id = random.randint(1, 100)
            yield {"customer_id": f"customer_{id}",
                   "name": f"{names[id % len(names)]}_{surnames[id % len(surnames)]}",
                   "age": (id + 18) % 81,
                   "country": countries[id % len(countries)]}

    @staticmethod
    def test_scenarios_views_1_filter(tables: list[Table]) -> Table:
        table = tables[0]
        return (table
                .filter((table["quantity"] % 2 == 0) & (table["price"] > 20)))

    @staticmethod
    def test_scenarios_views_2_mutate(tables: list[Table]) -> Table:
        return (tables[0]
                .mutate(order_expense=_.quantity * _.price * (1 - _.discount))
                .select(["order_id", "product", "order_expense"]))

    @staticmethod
    def test_scenarios_views_3_aggregate(tables: list[Table]) -> Table:
        return (tables[0]
                .group_by(_.product)
                .aggregate(mean_quantity=_.quantity.mean(), max_discount=_.discount.max())
                .select(["product", "mean_quantity", "max_discount"]))

    @staticmethod
    def test_scenarios_views_4_join(tables: list[Table]) -> Table:
        return (tables[0]
                .join(tables[1], "customer_id")
                # changed these to keep support for spark which doesn't work with joins + aggregations
                # .group_by(_.country, _.product)
                # .agg_regate(mean_age=_.age.mean(), max_price=_.price.max()))
                .filter((_.quantity + _.age) % 2 == 0)
                .select(["name", "product", "age", "quantity"]))

    @staticmethod
    def test_scenarios_views_5_window(tables: list[Table]) -> Table:
        # unsupported by spark
        # pyspark.sql.utils.AnalysisException: Non-time-based windows are not supported on streaming DataFrames/Datasets;
        return (tables[0]
                .mutate(trailing_prod_price_mean=_.price.mean()
                        .over(ibis.window(group_by=[_.product], preceding=3, following=0))))


class TestViewsNexmark:
    streams = [
        StreamTable(
            "auction",
            ibis.schema({"id": ibis.dtype("int64"),
                         "item_name": ibis.dtype("string"),
                         "description": ibis.dtype("string"),
                         "initial_bid": ibis.dtype("int64"),
                         "reserve": ibis.dtype("int64"),
                         "date_time": ibis.dtype("int64"),
                         # "date_time": ibis.dtype("timestamp"),
                         "expires": ibis.dtype("int64"),
                         # "expires": ibis.dtype("timestamp"),
                         "seller": ibis.dtype("int64"),
                         "category": ibis.dtype("int64"),
                         "extra": ibis.dtype("string")}),
            lambda: TestViewsNexmark.nexmark_generator("auction")
        ),
        StreamTable(
            "bid",
            ibis.schema({"auction": ibis.dtype("int64"),
                         "bidder": ibis.dtype("int64"),
                         "price": ibis.dtype("int64"),
                         "channel": ibis.dtype("string"),
                         "url": ibis.dtype("string"),
                         "date_time": ibis.dtype("int64"),
                         # "date_time": ibis.dtype("timestamp"),
                         "extra": ibis.dtype("string")}),
            lambda: TestViewsNexmark.nexmark_generator("bid")
        ),
        StreamTable(
            "person",
            ibis.schema({"id": ibis.dtype("int64"),
                         "name": ibis.dtype("string"),
                         "email_address": ibis.dtype("string"),
                         "credit_card": ibis.dtype("string"),
                         "city": ibis.dtype("string"),
                         "state": ibis.dtype("string"),
                         "date_time": ibis.dtype("int64"),
                         # "date_time": ibis.dtype("timestamp"),
                         "extra": ibis.dtype("string")}),
            lambda: TestViewsNexmark.nexmark_generator("person")
        )
    ]

    @staticmethod
    def nexmark_generator(name: str):
        # add external loop so it restarts in case file is over:
        # no need to specify file size here as if bigger than the file
        # below, it will just restart
        while True:
            with open(f"data/nexmark/{name}_10000000.csv", "r") as csvfile:
                reader = csv.DictReader(csvfile, quoting=csv.QUOTE_NONE)
                for row in reader:
                    for k, v in row.items():
                        try:
                            cast = int(v)
                        except ValueError:
                            try:
                                cast = float(v)
                            except ValueError:
                                cast = v
                        row[k] = cast
                    yield row

    @staticmethod
    def test_nexmark_query_1(tables: list[Table]) -> Table:
        return (tables[1]
                .mutate(dol_price=_.price * 0.85)
                .select(["auction", "price", "dol_price", "bidder", "date_time"]))

    @staticmethod
    def test_nexmark_query_2(tables: list[Table]) -> Table:
        return (tables[1]
                .filter((_.auction == 1007) | (_.auction == 1020) | (_.auction == 2001) | (_.auction == 2019) | (_.auction == 2087))
                .select(["auction", "price"]))

    @staticmethod
    def test_nexmark_query_3(tables: list[Table]) -> Table:
        auction, person = tables[0], tables[2]
        return (auction
                .join(person, auction["seller"] == person["id"])
                .filter((person["state"] == "or") | (person["state"] == "id") | (person["state"] == "ca"))
                .filter(auction["category"] == 10)
                .select(["name", "city", "state", "id"]))

    @staticmethod
    def test_nexmark_query_4(tables: list[Table]) -> Table:
        # unsupported by spark
        # pyspark.sql.utils.AnalysisException: Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;
        # double group-reduce is actually also what's in the original query, so makes no sense to change it for spark
        auction, bid = tables[0], tables[1]
        CURRENT_TIME = 2330277279926
        # CURRENT_TIME = ibis.literal(2330277279926).to_timestamp()
        return (auction
                .join(bid, bid["auction"] == auction["id"])
                .filter((_.date_time_right < _.expires) & (_.expires < CURRENT_TIME))
                .group_by([_.id, _.category])
                .aggregate(final_p=_.price.max())
                .group_by(_.category)
                .aggregate(avg_final_p=_.final_p.mean()))

    @staticmethod
    def test_nexmark_query_6(tables: list[Table]) -> Table:
        # unsupported by spark even after adding watermark to all tables and changing column types to timestamp
        # changed column types back to int64 because risingwave doesn't support timestamp instead, and it didn't work for spark anyway
        # pyspark.sql.utils.AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
        # changed the await_stream_query to use "complete" output mode for queries containing aggregation but still unsupported, as joins are only supported in "append" mode
        # pyspark.sql.utils.AnalysisException: Join between two streaming DataFrames/Datasets is not supported in Complete output mode, only in Append output mode;
        auction, bid = tables[0], tables[1]
        CURRENT_TIME = 2330277279926
        # CURRENT_TIME = ibis.literal(2330277279926).to_timestamp()
        w = ibis.window(group_by=[_.seller], preceding=9, following=0)
        return (auction
                .join(bid, bid["auction"] == auction["id"])
                .filter((_.date_time_right < _.expires) & (_.expires < CURRENT_TIME))
                .group_by([_.id, _.seller])
                .aggregate(final_p=_.price.max())
                .mutate(avg_final_p=_.final_p.mean().over(w)))


class TestViewsTpcH:
    streams = [
        StreamTable(
            "lineitem",
            ibis.schema({"orderkey": ibis.dtype("int64"),
                         "partkey": ibis.dtype("int64"),
                         "suppkey": ibis.dtype("int64"),
                         "linenumber": ibis.dtype("int64"),
                         "quantity": ibis.dtype("float64"),
                         "extendedprice": ibis.dtype("float64"),
                         "discount": ibis.dtype("float64"),
                         "tax": ibis.dtype("float64"),
                         "returnflag": ibis.dtype("string"),
                         "linestatus": ibis.dtype("string"),
                         "shipdate": ibis.dtype("string"),
                         "commitdate": ibis.dtype("string"),
                         "receiptdate": ibis.dtype("string"),
                         "shipinstruct": ibis.dtype("string"),
                         "shipmode": ibis.dtype("string"),
                         "comment": ibis.dtype("string")}),
            lambda: TestViewsTpcH.tpch_generator("lineitem")
        )]

    @staticmethod
    def tpch_generator(name: str):
        while True:
            with open(f"data/tpch/{name}_10000000.csv", "r") as csvfile:
                reader = csv.DictReader(csvfile, quoting=csv.QUOTE_NONE)
                for row in reader:
                    for k, v in row.items():
                        try:
                            cast = int(v)
                        except ValueError:
                            try:
                                cast = float(v)
                            except ValueError:
                                cast = v
                        row[k] = cast
                    yield row

    @staticmethod
    def test_tpch_query_1(tables: list[Table]) -> Table:
        # TODO: check different potential aggregation semantics for spark vs risingwave
        # which can affect results in aggregation-heavy queries such as this one, producing
        # different amounts of rows
        lineitem = tables[0]
        return (lineitem
                .filter(lineitem["shipdate"] <= "1998-11-01")
                .group_by(["returnflag", "linestatus"])
                .aggregate(
                    sum_qty=lineitem["quantity"].sum(),
                    sum_base_price=lineitem["extendedprice"].sum(),
                    sum_disc_price=(
                        lineitem["extendedprice"] * (1 - lineitem["discount"])).sum(),
                    sum_charge=(
                        lineitem["extendedprice"] * (1 - lineitem["discount"]) * (1 + lineitem["tax"])).sum(),
                    avg_qty=lineitem["quantity"].mean(),
                    avg_price=lineitem["extendedprice"].mean(),
                    avg_disc=lineitem["discount"].mean(),
                    # changed count star semantics for ibis 9.2.0
                    count_order=_.quantity.count()))

    @staticmethod
    def test_tpch_query_6(tables: list[Table]) -> Table:
        lineitem = tables[0]
        return (lineitem
                .filter((lineitem["shipdate"] >= "1994-01-01") &
                        (lineitem["shipdate"] < "1995-01-01") &
                        (lineitem["discount"] >= 0.05) &
                        (lineitem["discount"] <= 0.07) &
                        (lineitem["quantity"] < 24))
                .aggregate(revenue=(_.extendedprice * _.discount).sum()))
