package com.example.segmentation;

import com.example.segmentation.avro.Order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.clients.consumer.CloseOptions.timeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SampleOrderProducerIntegrationTest {

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

    private KafkaConsumer<String, Order> consumer;

    private String schemaRegistryUrl() {
        return "http://" + SCHEMA_REGISTRY.getHost() + ":" + SCHEMA_REGISTRY.getMappedPort(8081);
    }

    private AppConfig testConfig() {
        Dotenv dotenv = Dotenv.configure()
            .directory("/dev/null/..")
            .ignoreIfMissing()
            .load();

        return new AppConfig(dotenv) {
            @Override
            public Properties adminClientProperties() {
                var props = new Properties();
                props.put("bootstrap.servers", KAFKA.getBootstrapServers());
                return props;
            }

            @Override
            public java.util.Map<String, String> schemaRegistryConfig() {
                return java.util.Map.of("schema.registry.url", schemaRegistryUrl());
            }
        };
    }

    @BeforeAll
    void setUp() {
        CustomerSegmentationApp.createTopics(KAFKA.getBootstrapServers());

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "producer-test-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl());
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of(CustomerSegmentationTopology.INPUT_TOPIC));
    }

    @AfterAll
    void tearDown() {
        if (consumer != null) consumer.close(timeout(ofSeconds(5)));
        NETWORK.close();
    }

    @Test
    void producerSendsValidAvroOrders() {
        AppConfig config = testConfig();
        var props = SampleOrderProducer.buildProducerProperties(config);
        var random = new Random(42);

        try (var producer = new KafkaProducer<String, Order>(props)) {
            for (int i = 0; i < 5; i++) {
                Order order = SampleOrderProducer.generateOrder(random);
                producer.send(new ProducerRecord<>(
                    CustomerSegmentationTopology.INPUT_TOPIC,
                    order.getCustomerId(),
                    order
                ));
            }
            producer.flush();
        }

        List<Order> received = new ArrayList<>();

        await()
            .atMost(ofSeconds(30))
            .untilAsserted(() -> {
                ConsumerRecords<String, Order> records = consumer.poll(ofMillis(500));
                records.forEach(r -> received.add(r.value()));
                assertThat(received).hasSize(5);
            });

        var validCustomerIds = Arrays.asList(SampleOrderProducer.CUSTOMER_IDS);
        var validCategories = Arrays.asList(SampleOrderProducer.CATEGORIES);

        for (Order order : received) {
            assertThat(order.getOrderId()).isNotNull().startsWith("ord-");
            assertThat(order.getCustomerId()).isIn(validCustomerIds);
            assertThat(order.getCategory()).isIn(validCategories);
            assertThat(order.getAmount()).isBetween(5.0, 500.0);
            assertThat(order.getTimestamp()).isPositive();
        }
    }

    @Test
    void generateOrderProducesValidData() {
        var random = new Random(123);
        var validCustomerIds = Arrays.asList(SampleOrderProducer.CUSTOMER_IDS);
        var validCategories = Arrays.asList(SampleOrderProducer.CATEGORIES);

        for (int i = 0; i < 50; i++) {
            Order order = SampleOrderProducer.generateOrder(random);
            assertThat(order.getOrderId()).startsWith("ord-");
            assertThat(order.getCustomerId()).isIn(validCustomerIds);
            assertThat(order.getCategory()).isIn(validCategories);
            assertThat(order.getAmount()).isBetween(5.0, 500.0);
            assertThat(order.getTimestamp()).isPositive();
        }
    }
}
