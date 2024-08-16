from ibis import Table
from ibis import _
import ibis
import random

class TestViews:
    schema = ibis.schema({
            "order_id": ibis.dtype("string"),
            "product": ibis.dtype("string"),
            "quantity": ibis.dtype("int64")})
    
    @staticmethod
    def dict_generator():
        products = ["book", "shoes", "hat", "gloves", "scarf"]
        while True:
            yield {"order_id": f"order_{random.randint(1, 1000)}",
                   "product": random.choice(products),
                   "quantity": random.randint(1, 100),}
    
    @staticmethod
    def test_scenarios_views_1_filter(table: Table) -> Table:
        return (table
                .mutate(price=_.quantity * 2)
                .mutate(value=_.order_id))
    
    @staticmethod
    def test_scenarios_views_2_mutate(table: Table) -> Table:
        return (table
                .mutate(price=_.quantity * 2)
                .mutate(value=_.order_id))