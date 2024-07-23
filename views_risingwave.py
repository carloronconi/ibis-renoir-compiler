import ibis
import pandas as pd
from ibis import _
from ibis.backends.risingwave import Backend as RisingwaveBackend

con: RisingwaveBackend = RisingwaveBackend().connect(
                user="root",
                host="localhost",
                port=4566,
                database="dev")

# risingwave is running inside a docker container,
# so localhost becomes host.docker.internal
con.raw_sql(
            """CREATE TABLE IF NOT EXISTS table_kafka (
                   orderId integer,
                   category varchar,
                   merchantId integer,
                )
                WITH (
                   connector='kafka',
                   topic='order',
                   properties.bootstrap.server='host.docker.internal:9092',
                   scan.startup.mode='latest',
                   scan.startup.timestamp.millis='140000000'
                ) FORMAT PLAIN ENCODE JSON;
            """
            )
con.list_tables()

