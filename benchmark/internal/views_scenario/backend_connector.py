import abc
from ibis import Table, Schema
from typing import Callable

class BackendConnector(abc.ABC):
    @abc.abstractmethod
    def create_table(self, schema: Schema) -> None:
        pass

    @abc.abstractmethod
    def create_view(self, test_query: Callable[[Table], Table]) -> None:
        pass

    @abc.abstractmethod
    def await_stream_query(self) -> None:
        pass
