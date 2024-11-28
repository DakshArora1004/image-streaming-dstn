from kafka import KafkaConsumer
import text_dataset_pb2  # Generated from text_dataset.proto
import os

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'text_topic',  # Kafka topic for text data
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    fetch_max_wait_ms=100,
)

def consume_text(save_path='received_data'):
    os.makedirs(save_path, exist_ok=True)  # Ensure the save directory exists

    for message in consumer:
        # Deserialize protobuf message
        text_data = text_dataset_pb2.TextData()
        text_data.ParseFromString(message.value)

        # Optionally, process or save the text content
        filename = os.path.join(save_path, f"text_{text_data.id}.txt")
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(text_data.content)
        
        print(f"Received and saved text with ID {text_data.id} as {filename}")

# Example usage
consume_text()
