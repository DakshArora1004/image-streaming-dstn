from kafka import KafkaProducer
import text_dataset_pb2  # Generated from text_dataset.proto
import os

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='10.70.6.244:9092',
    value_serializer=lambda v: v,  # No need for additional serialization
)

def send_text_dataset(folder_path):
    for i, filename in enumerate(os.listdir(folder_path)):
        file_path = os.path.join(folder_path, filename)

        # Read the text content from the file
        with open(file_path, 'r', encoding='utf-8') as file:
            text_content = file.read()

        # Create and populate protobuf message
        text_data = text_dataset_pb2.TextData()
        text_data.id = i
        text_data.content = text_content
        text_data.metadata = filename  # Optional: add filename as metadata

        # Serialize the protobuf message
        serialized_data = text_data.SerializeToString()

        # Send to Kafka
        future = producer.send('text_topic', serialized_data)
        try:
            record_metadata = future.get(timeout=10)
            print(f"Sent text file {filename} with offset {record_metadata.offset}")
        except Exception as e:
            print(f"Failed to send text file {filename}: {e}")

    producer.flush()
    print("All text files sent.")

# Example usage
send_text_dataset('processed_data')
