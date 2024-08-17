from dataclasses import dataclass
from typing import Callable, Generator, Any, NoReturn
from ibis import Table
from ibis import _
import ibis
import random


class TestViews:
    @dataclass
    class StreamTable:
        name: str
        schema: ibis.Schema
        generator: Callable[[], Generator[dict[str, Any], Any, NoReturn]]

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
