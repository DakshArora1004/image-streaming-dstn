import os
import logging
from kafka import KafkaConsumer
import text_dataset_pb2  # Your protobuf message

# FUSE Directory Path
fuse_directory = '/mnt/fuse_directory'  # Same directory where the FUSE filesystem is mounted

# Kafka consumer setup
consumer = KafkaConsumer(
    'your_topic',  # Replace with your actual topic name
    bootstrap_servers=['your_kafka_broker'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='your_group'
)

# Log configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def write_to_fuse(data):
    """Write data to the FUSE filesystem (received_data.txt)"""
    try:
        with open(os.path.join(fuse_directory, 'received_data.txt'), 'a') as f:
            f.write(data + '\n')
    except Exception as e:
        logger.error(f"Error writing to FUSE: {e}")

def consume_messages():
    """Consume messages from Kafka and write to FUSE"""
    logger.info("Consumer started...")
    for message in consumer:
        # Deserialize the protobuf message
        data = message.value
        try:
            my_message = text_dataset_pb2.YourMessageType()  # Adjust for your protobuf message type
            my_message.ParseFromString(data)
            # Convert the message to a string (or any desired format)
            data_str = str(my_message)
            # Write the deserialized data to the FUSE filesystem
            write_to_fuse(data_str)
            logger.info(f"Data written to FUSE: {data_str}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

if __name__ == '__main__':
    consume_messages()
