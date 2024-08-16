from ibis import Table
from ibis import _

class TestViews:
    
    @staticmethod
    def test_scenarios_views_1_filter(table: Table) -> Table:
        return (table
                .mutate(price=_.quantity * 2)
                .mutate(value=_.order_id))