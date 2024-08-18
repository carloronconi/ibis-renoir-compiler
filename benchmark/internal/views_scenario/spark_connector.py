import os
import shutil
from pyspark.sql import SparkSession
from ibis import _
import ibis
import ibis.backends.pyspark
from .backend_connector import BackendConnector
from ibis import Table, Schema
from typing import Callable


class SparkConnector(BackendConnector):
    def __init__(self, source_topic_schemas: dict[str, Schema], sink_topic: str) -> None:
        scala_version = '2.12'
        spark_version = '3.1.2'
        # ensure match above values match the correct versions in pip
        packages = [
            f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
            'org.apache.kafka:kafka-clients:3.2.1'
        ]
        session = SparkSession.builder\
            .master("spark://local:7077")\
            .appName("ibis")\
            .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
        self.con: ibis.backends.pyspark.Backend = ibis.pyspark.connect(session, mode="streaming")
        self.source_topic_schemas = source_topic_schemas
        self.sink_topic = sink_topic
        self.tables = []

    def create_tables(self):
        for topic, schema in self.source_topic_schemas.items():
            self.tables.append(self.con.read_kafka(
                table_name=topic,
                auto_parse=True,
                schema=schema,
                # watermark is required by nexmark q6 but it still doesn't work when using it
                # watermark=ibis.watermark("date_time", ibis.interval(seconds=10)),
                options={
                    "kafka.bootstrap.servers": "localhost:9092",
                    "subscribe": topic,
                    "startingOffsets": "earliest",
                    "failOnDataLoss": "false"}))
        
    def create_view(self, test_query: Callable[[list[Table]], Table]):
        self.view = self.con.create_view(
            self.sink_topic + "_view",
            test_query(self.tables))
        
    def await_stream_query(self):
        shutil.rmtree("spark_checkpoint", ignore_errors=True)
        os.makedirs("spark_checkpoint")
        stream_query = self.con.to_kafka(
            self.view, 
            auto_format=True,
            options={"kafka.bootstrap.servers": "localhost:9092", 
                     "topic": self.sink_topic,
                     "checkpointLocation": "spark_checkpoint"}).start()
        print("Starting and awaiting stream query")
        stream_query.awaitTermination()
        print("Stream query terminated")
