from kafka import KafkaConsumer

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'text_topic1',  # Kafka topic for text data
    bootstrap_servers='10.70.6.244:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    fetch_max_wait_ms=100,
)

def consume_text():
    """
    Consume text data from Kafka and print it.
    """
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")

# Example usage
consume_text()
