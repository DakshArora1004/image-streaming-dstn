from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import image_dataset_pb2
import numpy as np
import cv2
import os

def create_kafka_consumer():
    consumer_properties = {
        'bootstrap.servers': '10.30.21.253:9092',  # Kafka broker address
        'auto.offset.reset': 'earliest',  # Start consuming from the earliest offset
        'enable.auto.commit': 'false',  # Disable auto commit
    }

    return FlinkKafkaConsumer(
        'flink_test2',  # Kafka topic name
        SimpleStringSchema(),  # Use simple string schema for raw data
        consumer_properties
    )

def deserialize_and_process_image(message):
    try:
        print(f"Received message: {message}")  # Log the raw message
        
        # Deserialize Protobuf message
        image_data = image_dataset_pb2.ImageData()
        image_data.ParseFromString(bytes(message, 'utf-8'))

        # Extract image bytes and filename
        np_array = np.frombuffer(image_data.image_bytes, np.uint8)
        image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

        # Apply grayscale transformation
        gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

        # Save the processed image
        save_path = 'processed_images'
        os.makedirs(save_path, exist_ok=True)
        filename = os.path.join(save_path, image_data.filename)
        cv2.imwrite(filename, gray_image)

        print(f"Processed and saved image: {image_data.filename}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    # Create the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Create Kafka consumer
    kafka_consumer = create_kafka_consumer()

    # Add the consumer as a source to the environment
    data_stream = env.add_source(kafka_consumer)

    # Process each message in the data stream
    data_stream.map(deserialize_and_process_image)

    # Execute the Flink job
    env.execute("Flink Kafka Image Processing")

# Run the job directly within the Python script
if __name__ == '__main__':
    main()
