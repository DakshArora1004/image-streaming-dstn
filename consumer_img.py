from kafka import KafkaConsumer
import image_dataset_pb2
import numpy as np
import cv2
import os

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'flink_test7',
    bootstrap_servers='10.30.21.253:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False ,
    fetch_max_wait_ms=100
)

def consume_images(save_path='received_images'):
    os.makedirs(save_path, exist_ok=True)
    for message in consumer:
        # Deserialize protobuf message
        image_data = image_dataset_pb2.ImageData()
        image_data.ParseFromString(message.value)

        # Convert bytes back to an image
        np_array = np.frombuffer(image_data.image_bytes, np.uint8)
        image = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

        # Optionally, display or save the image
        filename = os.path.join(save_path, image_data.filename)
        cv2.imwrite(filename, image)
        print(f"Received and saved image {image_data.filename}")

# Example usage
consume_images()

