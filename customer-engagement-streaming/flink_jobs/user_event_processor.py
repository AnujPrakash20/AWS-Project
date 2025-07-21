from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import os

def main():
    
    # Add Kafka connector JAR to the Flink configuration
    kafka_jar_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'jars', 'flink-sql-connector-kafka.jar')
    
    # Step 1: Set up environments
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    
    # Add Kafka connector JAR to table environment
    t_env.get_config().get_configuration().set_string("pipeline.jars", f"file://{kafka_jar_path}")

    print("🚀 Setting up Kafka connection to read your published messages...")

    # Step 2: Set Kafka source table (your original schema - this connects to your real topic!)
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
    
    print("✅ Kafka table connection established successfully!")
    print("📡 Connected to topic: user-events-topic")
    print("🏠 Bootstrap servers: b-1.usereventscluster.im0pvd.c4.kafka.ap-south-1.amazonaws.com:9092")
    print("👥 Consumer group: flink-table-group")
    print()

    # Step 3: Process your actual Kafka messages!
    print("📊 Processing your published messages from Kafka topic...")
    result_table = t_env.sql_query("SELECT * FROM kafka_user_events LIMIT 20")

    # Step 4: Print the real data from your Kafka topic
    t_env.to_changelog_stream(result_table).print()
    
    print()
    print("🎉 SUCCESS: Now reading from your actual Kafka topic!")
    print("📝 You should see the messages you published appearing above...")
    print("💡 The system is now processing real-time data from user-events-topic")
    
    # Execute the job
    env.execute("Kafka User Events - Reading Real Messages")

if __name__ == '__main__':
    main()
