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
                "quantity": ibis.dtype("int64")}),
            lambda: TestViewsCustom.orders_generator()
        )]

    @staticmethod
    def orders_generator():
        products = ["book", "shoes", "hat", "gloves", "scarf"]
        while True:
            yield {"order_id": f"order_{random.randint(1, 1000)}",
                   "product": random.choice(products),
                   "quantity": random.randint(1, 100), }

    @staticmethod
    def test_scenarios_views_1_filter(tables: list[Table]) -> Table:
        return (tables[0]
                .filter(_.quantity % 2 == 0))

    @staticmethod
    def test_scenarios_views_2_mutate(tables: list[Table]) -> Table:
        return (tables[0]
                .mutate(price=_.quantity * 2))


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

    # TODO: implement exception-capture for s2 like other harness
    # TODO: re-run nexmark queries for all scenarios due to q3 changes
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
