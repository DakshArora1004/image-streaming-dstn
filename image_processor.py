import os
import sys
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
import uuid
import numpy as np
import cv2
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_pipeline():
    # Get the path to your current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set up JAR paths
    kafka_jar = "/home/duckqck/flink-1.20.0/opt/flink-connector-kafka-3.3.0.jar"
    python_jar = "/home/duckqck/flink-1.20.0/opt/flink-python-1.20.0.jar"
    jars_path = f"file:{kafka_jar};file:{python_jar}"
    
    # Create environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Configure environment
    env.set_configuration_string("pipeline.jars", jars_path)
    env.set_configuration_string("taskmanager.memory.process.size", "4096m")
    
    # Add Python file for protobuf
    pb2_file = os.path.join(current_dir, "image_dataset_pb2.py")
    env.add_python_file(pb2_file)
    
    # Kafka properties
    properties = {
        'bootstrap.servers': '10.20.15.14:9092',
        'group.id': f'flink_image_processor_{uuid.uuid4().hex[:8]}',
        'auto.offset.reset': 'latest'
    }
    
    try:
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
        
    except Exception as e:
        logger.error(f"Error setting up Kafka source: {str(e)}")
        raise

    def process_image(serialized_data):
        try:
            # Import protobuf module
            import image_dataset_pb2
            
            # Parse protobuf message
            image_data = image_dataset_pb2.ImageData()
            image_data.ParseFromString(serialized_data.encode())
            
            # Convert to numpy array and decode
            np_array = np.frombuffer(image_data.image_bytes, np.uint8)
            image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
            
            # Convert to grayscale
            gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Save processed image
            processed_filename = f"processed_{int(time.time())}_{image_data.filename}"
            save_path = os.path.join(current_dir, "processed_images", processed_filename)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Save image
            cv2.imwrite(save_path, gray_image)
            
            return f"Successfully processed {image_data.filename}"
            
        except Exception as e:
            logger.error(f"Error in process_image: {str(e)}")
            return f"Error processing image: {str(e)}"

    # Create processing pipeline
    processed_stream = data_stream \
        .map(process_image, output_type=Types.STRING()) \
        .name("Image Processing")
    
    # Add sink
    processed_stream.print()

    return env

if __name__ == "__main__":
    try:
        env = create_pipeline()
        env.execute("Image Processing Pipeline")
        
    except Exception as e:
        logger.error(f"Error executing job: {str(e)}")
        raise
