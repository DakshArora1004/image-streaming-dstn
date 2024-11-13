from kafka import KafkaProducer
import image_dataset_pb2
import cv2
import os

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def send_image_dataset(folder_path):
    for i, filename in enumerate(os.listdir(folder_path)):
        image_path = os.path.join(folder_path, filename)
        
        # Read and encode the image
        image = cv2.imread(image_path)
        _, buffer = cv2.imencode('.jpg', image)

        # Create and populate protobuf message
        image_data = image_dataset_pb2.ImageData()
        image_data.image_bytes = buffer.tobytes()
        image_data.filename = filename
        image_data.id = i  # Optional: assign a unique ID

        # Serialize the message
        serialized_data = image_data.SerializeToString()

        # Send to Kafka
        producer.send('image_dataset', serialized_data)
        producer.flush()
        print(f"Sent image {filename}")

# Example usage
send_image_dataset('images')
