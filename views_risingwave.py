import ibis
import pandas as pd
from ibis import _
from ibis.backends.risingwave import Backend as RisingwaveBackend

con: RisingwaveBackend = RisingwaveBackend().connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev")

table = con.create_source(
    name="source_kafka",
    schema=ibis.schema(
        [
            ("orderId", "int64"),
            ("category", "string"),
            ("merchantId", "int64"),
        ]
    ),
    # overwrite=True,
    connector_properties={"connector": "kafka",
                          "topic": "order",
                          "properties.bootstrap.server": "localhost:9092",
                          "scan.startup.mode": "latest",
                          "scan.startup.timestamp.millis": "140000000"},
    data_format="PLAIN",
    encode_format="JSON"
)
# risingwave is running inside a docker container,
# so localhost becomes host.docker.internal
# the raw query is this, but con.create_table seems to create the same thing
# couldn't get it to work with risingwave in container, but it works with risingwave locally
# con.raw_sql(
#             """CREATE TABLE IF NOT EXISTS table_kafka (
#                    orderId integer,
#                    category varchar,
#                    merchantId integer,
#                 )
#                 WITH (
#                    connector='kafka',
#                    topic='order',
#                    properties.bootstrap.server='host.docker.internal:9092',
#                    scan.startup.mode='latest',
#                    scan.startup.timestamp.millis='140000000'
#                 ) FORMAT PLAIN ENCODE JSON;
#             """
#             )

print(table)

con.create_materialized_view("view_kafka", 
                             obj=table.mutate(value=_.category), 
                             overwrite=True)
con.create_sink("sink_kafka",
                sink_from="view_kafka",
                connector_properties={"connector": "kafka",
                                      "topic": "sink",
                                      "properties.bootstrap.server": "localhost:9092"},
                data_format="PLAIN",
                encode_format="JSON",
                encode_properties={"force_append_only": "true"})
print(con.list_tables())

