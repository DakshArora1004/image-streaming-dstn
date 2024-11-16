from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import sys

def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Define Kafka consumer properties
    kafka_consumer = FlinkKafkaConsumer(
        topics='test_topic1',  # Kafka topic name
        deserialization_schema=SimpleStringSchema(),  # Deserialize messages as strings
        properties={
            'bootstrap.servers': '10.30.21.253:9092',  # Replace with your Kafka broker
            'group.id': 'test_group',  # Consumer group ID
            'auto.offset.reset': 'earliest',  # Start reading from the beginning
        }
    )

    # Add Kafka source to the Flink environment
    data_stream = env.add_source(kafka_consumer)

    # Process data: simply convert text to uppercase
    processed_stream = data_stream.map(lambda x: x.upper())

    # Print processed data to stdout
    processed_stream.print()

    # Execute the Flink job
    env.execute("Simple Kafka-Flink Example")

if __name__ == "__main__":
    main()
