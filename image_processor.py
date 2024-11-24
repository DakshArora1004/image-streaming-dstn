from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import Configuration  # Changed from pyflink.common.config
import uuid
import image_dataset_pb2
import numpy as np
import cv2
import logging
import sys
import time
import os
logging.basicConfig(level=logging.INFO)


def create_pipeline():
    config = Configuration()
    
    # Get absolute paths for the JARs
    kafka_jar = "/home/duckqck/flink-1.20.0/opt/flink-connector-kafka-3.3.0.jar"
    python_jar = "/home/duckqck/flink-1.20.0/opt/flink-python-1.20.0.jar"
    
    # Set the jars using the absolute paths
    jars_path = f"file:{kafka_jar};file:{python_jar}"
    config.set_string("pipeline.jars", jars_path)
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)
    # Kafka properties
    properties = {
        'bootstrap.servers': '10.20.15.14:9092',
        'group.id': 'flink_image_processor',
        'auto.offset.reset': 'latest',
        'socket.timeout.ms': '5000',      # Add this
        'request.timeout.ms': '6000'      # Add this
    }

    # Create Kafka source
    kafka_source = KafkaSource.builder() \
        .set_topics("flink_test3") \
        .set_properties(properties) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create DataStream
    data_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="Kafka Source"
    )

    def process_image(serialized_data):
        try:
            # Deserialize Protobuf message
            image_data = image_dataset_pb2.ImageData()
            image_data.ParseFromString(serialized_data.encode())

            # Convert bytes to image
            np_array = np.frombuffer(image_data.image_bytes, np.uint8)
            image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

            # Process image
            gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

            # Save processed image
            processed_filename = f"processed_{image_data.filename}"
            cv2.imwrite(processed_filename, gray_image)

            return f"Successfully processed {image_data.filename} as {processed_filename}"
        except Exception as e:
            return f"Error processing image: {str(e)}"

    # Apply processing and add error handling
    processed_stream = data_stream.map(
        process_image,
        output_type=Types.STRING()
    ).name("Image Processing")  # Name the operator for better monitoring

    # Add sink operation
    processed_stream.print().name("Print Results")

    return env

if __name__ == "__main__":
    print("About to execute Flink job...")
    sys.stdout.flush()
    try:
        env = create_pipeline()  # Get the environment
        job_result = env.execute("Image Processing Pipeline")
        print(f"Job submitted with ID: {job_result.get_job_id()}")
    except Exception as e:
        print(f"Error executing job: {str(e)}")