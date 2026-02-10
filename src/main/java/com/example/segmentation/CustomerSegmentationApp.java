package com.example.segmentation;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerSegmentationApp {

    private static final Logger log = LoggerFactory.getLogger(
        CustomerSegmentationApp.class
    );

    static void createTopics(String bootstrapServers) {
        var config = new Properties();
        config.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );

        var requiredTopics = List.of(
            new NewTopic(
                CustomerSegmentationTopology.INPUT_TOPIC,
                1,
                (short) 1
            ),
            new NewTopic(
                CustomerSegmentationTopology.OUTPUT_TOPIC,
                1,
                (short) 1
            )
        );

        try (var admin = AdminClient.create(config)) {
            Set<String> existing = admin.listTopics().names().get();
            List<NewTopic> toCreate = requiredTopics
                .stream()
                .filter(t -> !existing.contains(t.name()))
                .collect(Collectors.toList());

            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get();
                log.info(
                    "Created topics: {}",
                    toCreate
                        .stream()
                        .map(NewTopic::name)
                        .collect(Collectors.joining(", "))
                );
            } else {
                log.info("All required topics already exist");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topics", e);
        }
    }

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";

        Properties props = new Properties();
        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            "customer-segmentation-app"
        );
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        createTopics(bootstrapServers);

        log.info("Schema Registry URL: {}", schemaRegistryUrl);
        var schemaRegistryConfig = java.util.Map.of(
            "schema.registry.url",
            schemaRegistryUrl
        );
        Topology topology = CustomerSegmentationTopology.build(
            schemaRegistryConfig
        );
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setStateListener((newState, oldState) ->
            log.info("Streams state transition: {} -> {}", oldState, newState)
        );

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                log.info("Shutting down streams application...");
                streams.close();
                latch.countDown();
            })
        );

        try {
            log.info("Starting customer segmentation streams application...");
            streams.start();
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
