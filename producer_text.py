from kafka import KafkaProducer
import text_dataset_pb2  # Generated from text_dataset.proto

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='10.70.6.244:9092',
    value_serializer=lambda v: v,  # No need for additional serialization
)

def send_hello_message():
    # Create and populate protobuf message
    text_data = text_dataset_pb2.TextData()
    text_data.id = 0
    text_data.content = "Hello"
    text_data.metadata = "hello_message"

    # Serialize the protobuf message
    serialized_data = text_data.SerializeToString()

    # Send to Kafka
    try:
        future = producer.send('text_topic1', serialized_data)
        record_metadata = future.get(timeout=10)
        print(f"Sent hello message with offset {record_metadata.offset}")
    except Exception as e:
        print(f"Failed to send hello message: {e}")
    
    producer.flush()
    print("Hello message sent.")

# Send the hello message
send_hello_message()
