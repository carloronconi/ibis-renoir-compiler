import abc
from ibis import Table, Schema
from typing import Callable

class BackendConnector(abc.ABC):
    @abc.abstractmethod
    def __init__(self, source_topic_schemas: dict[str, Schema], sink_topic: str) -> None:
        pass    

    @abc.abstractmethod
    def create_tables(self) -> None:
        pass

    @abc.abstractmethod
    def create_view(self, test_query: Callable[[list[Table]], Table]) -> None:
        pass

    @abc.abstractmethod
    def await_stream_query(self) -> None:
        pass
