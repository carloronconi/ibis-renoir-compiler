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


class TestViews:
    streams = [
        StreamTable(
            "orders",
            ibis.schema({
                "order_id": ibis.dtype("string"),
                "product": ibis.dtype("string"),
                "quantity": ibis.dtype("int64")}),
            lambda: TestViews.orders_generator()
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
                .filter(_.quantity % 2 == 0)
                .mutate(value=_.order_id))

    @staticmethod
    def test_scenarios_views_2_mutate(tables: list[Table]) -> Table:
        return (tables[0]
                .mutate(price=_.quantity * 2)
                .mutate(value=_.order_id))

    # TODO: define nexmark queries + schema and generator!


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
                         "expires": ibis.dtype("int64"),
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
            for row in open(f"data/nexmark/{name}_10000000.csv", "r"):
                yield row

    @staticmethod
    def test_nexmark_query_1(tables: list[Table]) -> Table:
        return (tables[1]
                .mutate(dol_price=_.price * 0.85)
                .select(["auction", "price", "dol_price", "bidder", "date_time"]))
