package com.example.flink;

import com.google.protobuf.InvalidProtocolBufferException;
import image_dataset.ImageDataOuterClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.util.Properties;
import javax.imageio.ImageIO;

public class KafkaImageProcessor {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.30.21.253:9092");
        properties.setProperty("group.id", "flink-image-consumer-debug"); // Changed group ID for debugging
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("request.timeout.ms", "120000");

        // Kafka consumer
        FlinkKafkaConsumer<ImageDataOuterClass.ImageData> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink_test7",
                new ImageDataDeserializationSchema(),
                properties
        );

        // Enable debug logging for Kafka consumer
        kafkaConsumer.setStartFromEarliest(); // Explicitly set to consume from the beginning

        // DataStream processing
        DataStream<ImageDataOuterClass.ImageData> imageStream = env.addSource(kafkaConsumer);

        imageStream.map(imageData -> {
            try {
                // Debugging: Log raw data
                System.out.println("Received ImageData: " + imageData);

                // Decode the image bytes to get resolution
                BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageData.getImageBytes().toByteArray()));
                if (image == null) {
                    System.err.println("Error decoding image for file: " + imageData.getFilename());
                    return null;
                }
                int width = image.getWidth();
                int height = image.getHeight();
                System.out.println("Image: " + imageData.getFilename() + ", Resolution: " + width + "x" + height);
            } catch (Exception e) {
                System.err.println("Error processing ImageData: " + e.getMessage());
                e.printStackTrace();
            }
            return imageData;
        }).name("Image Decoding");

        // Execute the Flink job
        env.execute("Kafka Image Processor Debugging");
    }

    public static class ImageDataDeserializationSchema implements DeserializationSchema<ImageDataOuterClass.ImageData> {

        @Override
        public ImageDataOuterClass.ImageData deserialize(byte[] message) throws InvalidProtocolBufferException {
            try {
                System.out.println("Raw message size: " + message.length);
                ImageDataOuterClass.ImageData data = ImageDataOuterClass.ImageData.parseFrom(message);
                System.out.println("Deserialized ImageData: " + data);
                return data;
            } catch (Exception e) {
                System.err.println("Error deserializing message: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }


        @Override
        public boolean isEndOfStream(ImageDataOuterClass.ImageData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<ImageDataOuterClass.ImageData> getProducedType() {
            return TypeInformation.of(ImageDataOuterClass.ImageData.class);
        }
    }
}
