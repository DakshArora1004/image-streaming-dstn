from kafka import KafkaConsumer
import os

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'pc2_demo',  # Kafka topic for text data
    bootstrap_servers='10.70.6.244:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    fetch_max_wait_ms=100,
)

# Set the GlusterFS mount point as the save path
glusterfs_mount_path = '/mnt/glusterfs/received_data'

def consume_and_store(save_path=glusterfs_mount_path):
    """
    Consume text data from Kafka, store it in a single file in the volume cluster, 
    and output the formatted message.
    """
    # Ensure the save directory exists
    os.makedirs(save_path, exist_ok=True)

    # Path to the single file where all data will be stored
    file_path = os.path.join(save_path, "all_data.txt")
    count = 1

    try:
        # Open the file in append mode
        with open(file_path, 'a', encoding='utf-8') as file:
            for message in consumer:
                # Decode the message value
                decoded_message = message.value.decode('utf-8')

                # Append the message to the file
                file.write(f"Image {count}: {decoded_message} cars\n")
                file.flush()  # Ensure the data is written to the disk

                # Print the formatted output
                print(f"Image {count} has {decoded_message} cars")
                count += 1
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
consume_and_store()
