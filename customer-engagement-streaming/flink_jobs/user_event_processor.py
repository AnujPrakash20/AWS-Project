from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import os

def main():
    
    #os.environ['PYFLINK_JAVA_OPTIONS'] = '-Dpipeline.jars=file:///Users/anujprakash/Desktop/Data-Engineering-Projects/customer-engagement-streaming/jars/flink-connector-kafka-3.0.0-fat.jar'

    # Step 1: Set up environments
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)

    # Step 2: Set Kafka source table
    t_env.execute_sql("""
        CREATE TABLE kafka_user_events (
            user_id INT,
            session_id STRING,
            event_type STRING,
            `timestamp` STRING,
            page STRING,
            `region` STRING,
            device STRING,
            `os` STRING,
            browser STRING,
            ab_test_group STRING,
            referral STRING,
            latency_ms INT,
            metadata ROW<
                user_agent STRING,
                ip_address STRING,
                campaign_id STRING,
                `language` STRING
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events-topic',
            'properties.bootstrap.servers' = 'b-1.usereventscluster.im0pvd.c4.kafka.ap-south-1.amazonaws.com:9092',
            'properties.group.id' = 'flink-table-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Step 3: Preview the Kafka data (SELECT *)
    result_table = t_env.sql_query("SELECT * FROM kafka_user_events")

    # Step 4: Print to console
    t_env.to_changelog_stream(result_table).print()

    env.execute("Kafka to Table - Preview All Data")

if __name__ == '__main__':
    main()
