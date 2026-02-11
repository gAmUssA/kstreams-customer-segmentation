package com.example.segmentation;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
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
        doCreateTopics(
            config,
            CustomerSegmentationTopology.INPUT_TOPIC,
            CustomerSegmentationTopology.OUTPUT_TOPIC,
            1,
            (short) 1
        );
    }

    static void createTopics(AppConfig config) {
        doCreateTopics(
            config.adminClientProperties(),
            config.getInputTopic(),
            config.getOutputTopic(),
            config.getTopicPartitions(),
            config.getTopicReplicationFactor()
        );
    }

    private static void doCreateTopics(
        Properties adminProps,
        String inputTopic,
        String outputTopic,
        int partitions,
        short replicationFactor
    ) {
        var requiredTopics = List.of(
            new NewTopic(inputTopic, partitions, replicationFactor),
            new NewTopic(outputTopic, partitions, replicationFactor)
        );

        try (var admin = AdminClient.create(adminProps)) {
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
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        AppConfig config = new AppConfig(dotenv);

        log.info("Starting in {} mode", config.getMode());
        log.info(
            "Bootstrap servers: {}",
            config.streamsProperties().get("bootstrap.servers")
        );
        log.info(
            "Schema Registry URL: {}",
            config.schemaRegistryConfig().get("schema.registry.url")
        );

        createTopics(config);

        Topology topology = CustomerSegmentationTopology.build(
            config.schemaRegistryConfig(),
            config.getInputTopic(),
            config.getOutputTopic()
        );
        KafkaStreams streams = new KafkaStreams(
            topology,
            config.streamsProperties()
        );

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
