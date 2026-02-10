package com.example.segmentation;

import com.example.segmentation.avro.CustomerSegment;
import com.example.segmentation.avro.Order;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.consumer.CloseOptions.timeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CustomerSegmentationIntegrationTest {

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final ConfluentKafkaContainer KAFKA =
        new ConfluentKafkaContainer("confluentinc/cp-kafka:8.1.1")
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @SuppressWarnings("resource")
    @Container
    private static final GenericContainer<?> SCHEMA_REGISTRY =
        new GenericContainer<>("confluentinc/cp-schema-registry:8.1.1")
            .withNetwork(NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://kafka:9093"
            )
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    private KafkaStreams streams;
    private KafkaProducer<String, Order> producer;
    private KafkaConsumer<String, CustomerSegment> consumer;

    private String schemaRegistryUrl() {
        return (
            "http://" +
            SCHEMA_REGISTRY.getHost() +
            ":" +
            SCHEMA_REGISTRY.getMappedPort(8081)
        );
    }

    @BeforeAll
    void setUp() {
        String bootstrapServers = KAFKA.getBootstrapServers();
        String srUrl = schemaRegistryUrl();

        CustomerSegmentationApp.createTopics(bootstrapServers);

        var srConfig = Map.of("schema.registry.url", srUrl);
        var topology = CustomerSegmentationTopology.build(srConfig);

        Properties streamsProps = new Properties();
        streamsProps.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            "integration-test-" + UUID.randomUUID()
        );
        streamsProps.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streams = new KafkaStreams(topology, streamsProps);
        streams.start();

        Properties producerProps = new Properties();
        producerProps.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );
        producerProps.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class
        );
        producerProps.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            KafkaAvroSerializer.class
        );
        producerProps.put(
            KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            srUrl
        );
        producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            bootstrapServers
        );
        consumerProps.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "test-consumer-" + UUID.randomUUID()
        );
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class
        );
        consumerProps.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            KafkaAvroDeserializer.class
        );
        consumerProps.put(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            srUrl
        );
        consumerProps.put(
            KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
            true
        );
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(CustomerSegmentationTopology.OUTPUT_TOPIC));
    }

    @AfterAll
    void tearDown() {
        if (streams != null) streams.close(ofSeconds(5));
        if (producer != null) producer.close(ofSeconds(5));
        if (consumer != null) consumer.close(timeout(ofSeconds(5)));
        NETWORK.close();
    }

    @Test
    void singleOrderProducesNewSegment() {
        String customerId = "it-cust-" + UUID.randomUUID();
        producer.send(
            new ProducerRecord<>(
                CustomerSegmentationTopology.INPUT_TOPIC,
                customerId,
                new Order("o1", customerId, 50.0, 1000L, "books")
            )
        );
        producer.flush();

        Map<String, CustomerSegment> results = new ConcurrentHashMap<>();

        await()
            .atMost(ofSeconds(30))
            .untilAsserted(() -> {
                ConsumerRecords<String, CustomerSegment> records =
                    consumer.poll(ofMillis(500));
                records.forEach(r -> results.put(r.key(), r.value()));
                assertThat(results).containsKey(customerId);
            });

        CustomerSegment segment = results.get(customerId);
        assertThat(segment.getCustomerSegment()).isEqualTo("New");
        assertThat(segment.getPurchaseBehavior()).isEqualTo("Single-Category");
        assertThat(segment.getOrderCount()).isEqualTo(1);
        assertThat(segment.getTotalSpent()).isEqualTo(50.0);
    }

    @Test
    void multipleOrdersBuildUpToVip() {
        String customerId = "it-vip-" + UUID.randomUUID();
        String[] categories = {
            "books",
            "electronics",
            "clothing",
            "food",
            "books",
            "electronics",
            "clothing",
            "food",
            "books",
            "electronics",
        };

        for (int i = 0; i < 10; i++) {
            producer.send(
                new ProducerRecord<>(
                    CustomerSegmentationTopology.INPUT_TOPIC,
                    customerId,
                    new Order(
                        "o" + i,
                        customerId,
                        100.0,
                        1000L * (i + 1),
                        categories[i]
                    )
                )
            );
        }
        producer.flush();

        Map<String, CustomerSegment> results = new ConcurrentHashMap<>();

        await()
            .atMost(ofSeconds(30))
            .untilAsserted(() -> {
                ConsumerRecords<String, CustomerSegment> records =
                    consumer.poll(ofMillis(500));
                records.forEach(r -> results.put(r.key(), r.value()));

                assertThat(results).containsKey(customerId);
                assertThat(
                    results.get(customerId).getCustomerSegment()
                ).isEqualTo("VIP");
            });

        CustomerSegment segment = results.get(customerId);
        assertThat(segment.getTotalSpent()).isEqualTo(1000.0);
        assertThat(segment.getOrderCount()).isEqualTo(10);
        assertThat(segment.getPurchaseBehavior()).isEqualTo("Diverse");
        assertThat(segment.getCategoriesPurchased()).isEqualTo(4);
        assertThat(segment.getAvgOrderValue()).isEqualTo(100.0);
    }
}
