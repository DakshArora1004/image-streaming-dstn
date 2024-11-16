from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import cv2
import numpy as np
import os
from pyflink.common.serialization import DeserializationSchema
from pyflink.common.typeinfo import Types
import image_dataset_pb2
class ProtobufDeserializationSchema(DeserializationSchema):
    def deserialize(self, message):
        image_data = image_dataset_pb2.ImageData()
        image_data.ParseFromString(message)
        return image_data

    def is_end_of_stream(self, next_element):
        return False

    def get_produced_type(self):
        return Types.PICKLED_BYTE_ARRAY()


def process_image(image_bytes):
    # Convert bytes back to an image
    np_array = np.frombuffer(image_bytes, np.uint8)
    image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
    
    # Convert image to grayscale
    grayscale_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # Encode back to bytes
    _, buffer = cv2.imencode('.jpg', grayscale_image)
    return buffer.tobytes()

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Kafka consumer setup
    kafka_consumer = FlinkKafkaConsumer(
    topics='flink_test',
    deserialization_schema=ProtobufDeserializationSchema(),
    properties={
        'bootstrap.servers': '10.30.21.253:9092',
    }
)


    # Kafka producer setup
    kafka_producer = FlinkKafkaProducer(
        topic='processed_images',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': '10.30.21.253:9092'}
    )

    # Define processing logic
    def grayscale_processing(stream):
        return stream.map(lambda message: process_image(message.value))

    # Define Flink pipeline
    image_stream = env.add_source(kafka_consumer)
    processed_stream = grayscale_processing(image_stream)
    processed_stream.add_sink(kafka_producer)

    # Execute the Flink job
    env.execute("Image Grayscale Processing")

if __name__ == "__main__":
    main()
