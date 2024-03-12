from pyflink.table import EnvironmentSettings, TableEnvironment
from kafka import KafkaConsumer

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch

"""
https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/try-flink/datastream/
base apache flink tutorial: Java

https://ibis-project.org/tutorials/open-source-software/apache-flink/1_single_feature
tutorial for using apache flink with ibis

https://github.com/ibis-project/ibis-flink-tutorial
github repo hosting the tutorial to be cloned: contains jupyter notebook with more complete guide
and docker compose to create Kafka topics, generate sample data, and launch a Flink cluster in the background.

So to make the following work:
1. ensure you have a .venv (if not: bottom-right corner / add new interpreter / local / virtualenv / new and add with 
defaults)
2. let PyCharm install imports by clicking on red underlined imports
3. PyCharm installed the wrong kafka! Do: `pip uninstall kafka` and `pip install kafka-python`
4. start the docker desktop application
5. cd to the [cloned notebook](https://github.com/ibis-project/ibis-flink-tutorial) and do `docker compose up -d`
6. run this script and ensure that it prints kafka topic contents
7. do `wget -N https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-conn
ector-kafka-3.0.2-1.18.jar`
"""


def conn():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    connection = ibis.flink.connect(table_env)
    print(connection)


def ibis_flink_tutorial():
    consumer = KafkaConsumer("transaction", auto_offset_reset="earliest")
    for _, msg in zip(range(10), consumer):
        # this ensures that messages exist in the `transaction` topic before
        # proceeding
        print(msg)

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set("parallelism.default", "1")
    connection = ibis.flink.connect(table_env)

    connection.raw_sql("ADD JAR './flink-sql-connector-kafka-3.0.2-1.18.jar'")

    # Connect to data source

    source_schema = sch.Schema(
        {
            "user_id": dt.int64,
            "trans_date_trans_time": dt.timestamp(scale=3),
            "cc_num": dt.int64,
            "amt": dt.float64,
            "trans_num": dt.str,
            "merchant": dt.str,
            "category": dt.str,
            "is_fraud": dt.int32,
            "first": dt.str,
            "last": dt.str,
            "dob": dt.str,
            "zipcode": dt.str,
        }
    )

    # Configure the source table with Kafka connector properties.
    source_configs = {
        "connector": "kafka",
        "topic": "transaction",
        "properties.bootstrap.servers": "localhost:9092",
        "properties.group.id": "consumer_group_0",
        "scan.startup.mode": "earliest-offset",
        "format": "json",
    }

    # Create the source table using the defined schema, Kafka connector properties,
    # and set watermarking for real-time processing with a 15-second allowed
    # lateness.
    source_table = connection.create_table(
        "transaction",
        schema=source_schema,
        tbl_properties=source_configs,
        watermark=ibis.watermark(
            time_col="trans_date_trans_time", allowed_delay=ibis.interval(seconds=15)
        ),
    )

    print(connection.list_tables())

    # Create transformations

    user_trans_amt_last_360m_agg = source_table[  # .select( # pandas syntax is equivalent to select
        source_table.user_id,
            # Calculate the average transaction amount over the past six hours
        source_table.amt.mean()
        .over(
            ibis.window(
                group_by=source_table.user_id,
                order_by=source_table.trans_date_trans_time,
                range=(-ibis.interval(minutes=360), 0),
            )
        )
        .name("user_mean_trans_amt_last_360min"),
            # Calculate the total transaction count over the past six hours
        source_table.amt.count()
        .over(
            ibis.window(
                group_by=source_table.user_id,
                order_by=source_table.trans_date_trans_time,
                range=(-ibis.interval(minutes=360), 0),
            )
        )
        .name("user_trans_count_last_360min"),
        source_table.trans_date_trans_time,
    ]  # )

    windowed_stream = source_table.window_by(
        time_col=source_table.trans_date_trans_time,
    ).tumble(window_size=ibis.interval(minutes=360))

    user_trans_amt_last_360m_agg_windowed_stream = windowed_stream.group_by(
        ["window_start", "window_end", "user_id"]
    ).agg(
        user_mean_trans_amt_last_360min=windowed_stream.amt.mean(),
        user_trans_count_last_360min=windowed_stream.amt.count(),
    )

    # Connect to a data sink

    sink_schema = sch.Schema(
        {
            "user_id": dt.int64,
            "user_mean_trans_amt_last_360min": dt.float64,
            "user_trans_count_last_360min": dt.int64,
            "trans_date_trans_time": dt.timestamp(scale=3),
        }
    )

    # Configure the sink table with Kafka connector properties for writing results.
    sink_configs = {
        "connector": "kafka",
        "topic": "user_trans_amt_last_360min",
        "properties.bootstrap.servers": "localhost:9092",
        "format": "json",
    }

    sink_table = connection.create_table(
        "user_trans_amt_last_360min",
        schema=sink_schema,
        tbl_properties=sink_configs,
    )

    # Write query results into sink

    connection.insert("user_trans_amt_last_360min",
                      user_trans_amt_last_360m_agg)

    # Windowing TVS

    sink_schema = sch.Schema(
        {
            "window_start": dt.timestamp(scale=3),
            "window_end": dt.timestamp(scale=3),
            "user_id": dt.int64,
            "user_mean_trans_amt_last_360min": dt.float64,
            "user_trans_count_last_360min": dt.int64,
        }
    )

    # Configure the sink table with Kafka connector properties for writing results.
    sink_configs = {
        "connector": "kafka",
        "topic": "user_trans_amt_last_360min_windowed",
        "properties.bootstrap.servers": "localhost:9092",
        "format": "json",
    }

    sink_table = connection.create_table(
        "user_trans_amt_last_360min_windowed",
        schema=sink_schema,
        tbl_properties=sink_configs,
    )

    connection.insert(
        "user_trans_amt_last_360min_windowed", user_trans_amt_last_360m_agg_windowed_stream
    )

    # Read results from kafka

    consumer = KafkaConsumer(
        "user_trans_amt_last_360min"
    )  # or "user_trans_amt_last_360min_windowed"
    for _, msg in zip(range(10), consumer):
        print(msg)

    consumer = KafkaConsumer(
        "user_trans_amt_last_360min_windowed"
    )
    for _, msg in zip(range(10), consumer):
        print(msg)


if __name__ == '__main__':
    ibis_flink_tutorial()
