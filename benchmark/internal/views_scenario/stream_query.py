import traceback
from .spark_connector import SparkConnector
from .risingwave_connector import RisingwaveConnector
from .test import TestViewsCustom
from typing import Callable
from ibis import Table, Schema
import multiprocessing as mp


def create_stream_query(backend: str,
                        source_topic_schemas: dict[str, Schema],
                        sink_topic: str,
                        test_query: Callable[[list[Table]], Table],
                        exception_pipe) -> None:
    if backend == "spark":
        connector = SparkConnector(source_topic_schemas, sink_topic)
    elif backend == "risingwave":
        connector = RisingwaveConnector(source_topic_schemas, sink_topic)
    else:
        raise ValueError("Unknown backend!")

    try:
        connector.create_tables()
        connector.create_view(test_query)
        connector.await_stream_query()
    except Exception as e:
        exception_pipe.send(" ".join(traceback.format_exception(e))
                            .replace(",", "COMMA_ESCAPE")
                            .replace("\n", "NEWLINE_ESCAPE"))
