package com.example;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.bytedeco.opencv.opencv_core.Mat;
import org.bytedeco.opencv.opencv_imgcodecs.Imgcodecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.bytedeco.opencv.global.opencv_imgproc.*;

public class ImageProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ImageProcessor.class);

    public static void main(String[] args) throws Exception {
        // Set up Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Kafka source configuration
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.20.15.14:9092");
        properties.setProperty("group.id", "flink_image_processor_" + UUID.randomUUID().toString().substring(0, 8));

        // Create Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("bootstrap.servers"))
                .setTopics("flink_test3")
                .setGroupId(properties.getProperty("group.id"))
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(org.apache.flink.api.common.serialization.SimpleStringSchema.class)
                .build();

        // Create the data stream
        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Process the images
        stream.map(new ImageProcessingFunction())
                .name("Image Processing")
                .print();

        // Execute the job
        env.execute("Flink Image Processing Job");
    }

    private static class ImageProcessingFunction implements MapFunction<String, String> {
        @Override
        public String map(String value) throws Exception {
            try {
                // Parse protobuf message
                ImageDataOuterClass.ImageData imageData = ImageDataOuterClass.ImageData.parseFrom(
                        value.getBytes()
                );

                // Create output directory if it doesn't exist
                Path outputDir = Paths.get("processed_images");
                Files.createDirectories(outputDir);

                // Convert bytes to OpenCV Mat
                Mat image = new Mat(1, imageData.getImageBytes().size(), org.bytedeco.opencv.global.opencv_core.CV_8UC1);
                image.data().put(imageData.getImageBytes().toByteArray());
                Mat decodedImage = Imgcodecs.imdecode(image, Imgcodecs.IMREAD_COLOR);

                // Convert to grayscale
                Mat grayImage = new Mat();
                cvtColor(decodedImage, grayImage, COLOR_BGR2GRAY);

                // Generate output filename
                String outputFilename = String.format("processed_%d_%s",
                        Instant.now().getEpochSecond(),
                        imageData.getFilename()
                );
                Path outputPath = outputDir.resolve(outputFilename);

                // Save the processed image
                Imgcodecs.imwrite(outputPath.toString(), grayImage);

                LOG.info("Successfully processed: {}", imageData.getFilename());
                return String.format("Successfully processed %s as %s",
                        imageData.getFilename(),
                        outputFilename
                );

            } catch (InvalidProtocolBufferException e) {
                LOG.error("Failed to parse protobuf message", e);
                return "Error: Failed to parse protobuf message - " + e.getMessage();
            } catch (Exception e) {
                LOG.error("Error processing image", e);
                return "Error: Processing failed - " + e.getMessage();
            }
        }
    }
}