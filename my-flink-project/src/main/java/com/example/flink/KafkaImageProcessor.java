package com.example.flink;

import com.google.protobuf.InvalidProtocolBufferException;
import image_dataset.ImageDataOuterClass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Properties;

public class KafkaImageProcessor {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.70.6.244:9092");
        properties.setProperty("group.id", "flink-image-consumer-debug");
        properties.setProperty("auto.offset.reset", "earliest");

        // Kafka consumer
        FlinkKafkaConsumer<ImageDataOuterClass.ImageData> kafkaConsumer = new FlinkKafkaConsumer<>(
                "flink_test11",
                new ImageDataDeserializationSchema(),
                properties
        );

        kafkaConsumer.setStartFromEarliest();

        // DataStream processing
        DataStream<ImageDataOuterClass.ImageData> imageStream = env.addSource(kafkaConsumer);

        imageStream.map(imageData -> {
            try {
                // Decode the image bytes
                BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageData.getImageBytes().toByteArray()));
                if (image == null) {
                    System.err.println("Error decoding image for file: " + imageData.getFilename());
                    return null;
                }

                // Log image resolution
                int width = image.getWidth();
                int height = image.getHeight();
                System.out.println("Image: " + imageData.getFilename() + ", Resolution: " + width + "x" + height);

                // Send image to Flask API
                String apiUrl = "http://127.0.0.1:5000/detect";
                String response = sendImageToApi(apiUrl, imageData.getFilename(), imageData.getImageBytes().toByteArray());
                System.out.println("API Response for " + imageData.getFilename() + ": " + response);

            } catch (Exception e) {
                System.err.println("Error processing ImageData: " + e.getMessage());
                e.printStackTrace();
            }
            return imageData;
        }).name("Image Processing");

        // Execute the Flink job
        env.execute("Kafka Image Processor with Flask API");
    }

public static String sendImageToApi(String apiUrl, String filename, byte[] imageData) {
    // Use a boundary format more similar to curl's
    String boundary = "---------------------------" + System.currentTimeMillis();
    String LINE_FEED = "\r\n";
    HttpURLConnection connection = null;

    try {
        // Debug info
        System.out.println("\n=== Starting API request ===");
        System.out.println("Image data size: " + imageData.length + " bytes");

        // Prepare the multipart form data first
        ByteArrayOutputStream requestBody = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(requestBody, "UTF-8"), true);
            
        writer.append("--").append(boundary).append(LINE_FEED);
        writer.append("Content-Disposition: form-data; name=\"image\"; filename=\"").append(filename).append("\"").append(LINE_FEED);
        writer.append("Content-Type: application/octet-stream").append(LINE_FEED);
        writer.append(LINE_FEED);
        writer.flush();

        // Write image data
        requestBody.write(imageData);
        
        writer.append(LINE_FEED);
        writer.append("--").append(boundary).append("--").append(LINE_FEED);
        writer.flush();

        // Get the complete request body
        byte[] requestBodyBytes = requestBody.toByteArray();
        
        // Set up connection
        URL url = new URL(apiUrl);
        connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setUseCaches(false);
        
        // Important: Set Content-Length header
        connection.setRequestProperty("Content-Length", String.valueOf(requestBodyBytes.length));
        connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

        // Debug headers
        System.out.println("\n=== Request Headers ===");
        connection.getRequestProperties().forEach((key, value) -> 
            System.out.println(key + ": " + value));

        // Write to connection
        try (OutputStream outputStream = connection.getOutputStream()) {
            outputStream.write(requestBodyBytes);
            outputStream.flush();
        }

        // Debug response
        int responseCode = connection.getResponseCode();
        System.out.println("\n=== Response ===");
        System.out.println("Response Code: " + responseCode);
        
        // Handle response
        if (responseCode != HttpURLConnection.HTTP_OK) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getErrorStream()))) {
                StringBuilder errorResponse = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    errorResponse.append(line);
                }
                System.err.println("Error Response Body: " + errorResponse);
                return "Error: " + errorResponse.toString();
            }
        }

        // Read success response
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            System.out.println("Success Response Body: " + response.toString());
            return response.toString();
        }

    } catch (Exception e) {
        System.err.println("\n=== Exception ===");
        System.err.println("API call failed: " + e.getMessage());
        e.printStackTrace();
        return "Error: " + e.getMessage();

    } finally {
        if (connection != null) {
            connection.disconnect();
        }
    }
}

    public static class ImageDataDeserializationSchema implements DeserializationSchema<ImageDataOuterClass.ImageData> {

        @Override
        public ImageDataOuterClass.ImageData deserialize(byte[] message) throws InvalidProtocolBufferException {
            return ImageDataOuterClass.ImageData.parseFrom(message);
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

