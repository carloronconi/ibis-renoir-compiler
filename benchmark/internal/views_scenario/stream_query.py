from .spark_connector import SparkConnector
from .risingwave_connector import RisingwaveConnector
from .test import TestViews
from typing import Callable
from ibis import Table, Schema


def create_stream_query(backend: str, source_topic: str, sink_topic: str, 
                        source_schema: Schema, test_query: Callable[[Table], Table]):
    if backend == "spark":
        connector = SparkConnector(source_topic, sink_topic)
    elif backend == "risingwave":
        connector = RisingwaveConnector(source_topic, sink_topic)
    else:
        raise ValueError("Unknown backend!")
    
    connector.create_table(source_schema)
    connector.create_view(test_query)
    connector.await_stream_query()
