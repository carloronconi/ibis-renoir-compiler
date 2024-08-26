import ibis.backends.flink
from ibis import _
import ibis
from .backend_connector import BackendConnector
from ibis import Table, Schema
from typing import Callable
from ..backend_benchmark import FlinkBenchmark

class FlinkConnector(BackendConnector):
    def __init__(self, source_topic_schemas: dict[str, Schema], sink_topic: str) -> None:
        self.con: ibis.backends.flink.Backend = FlinkBenchmark.get_backend_con()
        self.con.raw_sql("ADD JAR 'flink-sql-connector-kafka-3.2.0-1.18.jar'")
        self.source_topic_schemas = source_topic_schemas
        self.sink_topic = sink_topic
        self.tables = []
        self.query_result = None

    def create_tables(self):
        for topic, schema in self.source_topic_schemas.items():
            self.tables.append(self.con.create_table(
                name=topic,
                schema=schema,
                tbl_properties={"connector": "kafka",
                                "topic": topic,
                                "properties.bootstrap.servers": "localhost:9092",
                                "properties.group.id": "test",
                                "scan.startup.mode": "earliest-offset",
                                "format": "json"}
            ))
        
    def create_view(self, test_query: Callable[[list[Table]], Table]):
        # schema required for flink and test query used later
        # workflow here: https://ibis-project.org/posts/flink-announcement/
        self.query_result = test_query(self.tables)
        self.view = self.con.create_table(
            name=self.sink_topic,
            schema=self.query_result.schema(),
            tbl_properties={"connector": "kafka",
                            "topic": self.sink_topic,
                            "properties.bootstrap.servers": "localhost:9092",
                            "format": "json"}
        )
        
    def await_stream_query(self):
        # again, special workflow for flink
        self.con.insert(self.sink_topic, self.query_result)
