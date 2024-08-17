from .spark_connector import SparkConnector
from .risingwave_connector import RisingwaveConnector
from .test import TestViews
from typing import Callable
from ibis import Table, Schema


def create_stream_query(backend: str, 
                        source_topic_schemas: dict[str, Schema], 
                        sink_topic: str, 
                        test_query: Callable[[list[Table]], Table]):
    if backend == "spark":
        connector = SparkConnector(source_topic_schemas, sink_topic)
    elif backend == "risingwave":
        connector = RisingwaveConnector(source_topic_schemas, sink_topic)
    else:
        raise ValueError("Unknown backend!")
    
    connector.create_tables()
    connector.create_view(test_query)
    connector.await_stream_query()
