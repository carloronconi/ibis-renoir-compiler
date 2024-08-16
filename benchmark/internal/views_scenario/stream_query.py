from .spark_connector import SparkConnector
from .risingwave_connector import RisingwaveConnector
from .test import TestViews


def create_stream_query(backend: str, source_topic: str, sink_topic: str, test_query_name: str):
    if backend == "spark":
        connector = SparkConnector(source_topic, sink_topic)
    elif backend == "risingwave":
        connector = RisingwaveConnector(source_topic, sink_topic)
    else:
        raise ValueError("Unknown backend!")
    
    test_query = next(method for name, method in TestViews.__dict__.items() if test_query_name == name)

    connector.create_table()
    connector.create_view(test_query)
    connector.await_stream_query()
