from ibis import Table
from ibis import _
import ibis

class TestViews:
    schema = ibis.schema({
            "order_id": ibis.dtype("string"),
            "product": ibis.dtype("string"),
            "quantity": ibis.dtype("int64")})
    
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