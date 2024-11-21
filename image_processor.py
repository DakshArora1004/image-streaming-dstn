from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import KafkaSource
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
import uuid
import image_dataset_pb2
import numpy as np
import cv2

# Create a StreamExecutionEnvironment
env = StreamExecutionEnvironment.get_execution_environment()

# Adding the jar to my streaming environment
env.add_jars("file:///home/pythonProject/UseCases/flink-sql-connector-kafka.jar")

properties = {
    'bootstrap.servers': 'your-bootstrap-url:9092',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'security.protocol': 'SASL_SSL',
    'sasl.jaas.config': "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required \
    username='your-username' \
    password='your-password';",
    'group.id': 'observability',
}

earliest = False
offset = KafkaOffsetsInitializer.earliest() if earliest else KafkaOffsetsInitializer.latest()

# Create a Kafka Source
kafka_source = KafkaSource.builder() \
    .set_topics("iot") \
    .set_properties(properties) \
    .set_starting_offsets(offset) \
    .set_value_only_deserializer(SimpleStringSchema())\
    .build()

# Create a DataStream from the Kafka source and assign timestamps and watermarks
data_stream = env.from_source(kafka_source, WatermarkStrategy.for_monotonous_timestamps(), "Kafka Source")

# Print line for readability in the console
print("start reading data from kafka")

# Processing Function
def process_image(serialized_data):
    # Deserialize Protobuf message
    image_data = image_dataset_pb2.ImageData()
    image_data.ParseFromString(serialized_data.encode())  # Convert to bytes

    # Convert bytes back to an image
    np_array = np.frombuffer(image_data.image_bytes, np.uint8)
    image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

    # Example Processing: Convert to grayscale
    gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Optionally save the processed image with a unique name
    processed_filename = f"processed_{image_data.filename}"
    cv2.imwrite(processed_filename, gray_image)

    # Return a log or message for the processed image
    return f"Processed {image_data.filename} as {processed_filename}"

# Apply the processing function to the data stream
data_stream.map(
    lambda x: process_image(x), 
    output_type=Types.STRING()
).print()

# Execute the Flink pipeline
env.execute("Kafka Source Example")
