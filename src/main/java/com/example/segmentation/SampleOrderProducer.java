package com.example.segmentation;

import com.example.segmentation.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.github.cdimascio.dotenv.Dotenv;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleOrderProducer {

    private static final Logger log = LoggerFactory.getLogger(
        SampleOrderProducer.class
    );

    static final String[] CUSTOMER_IDS = {
        "cust-001",
        "cust-002",
        "cust-003",
        "cust-004",
        "cust-005",
        "cust-006",
        "cust-007",
        "cust-008",
        "cust-009",
        "cust-010",
    };

    static final String[] CATEGORIES = {
        "books",
        "electronics",
        "clothing",
        "food",
        "sports",
        "home",
        "toys",
        "beauty",
    };

    private static final double MIN_AMOUNT = 5.0;
    private static final double MAX_AMOUNT = 500.0;
    private static final long SEND_INTERVAL_MS = 1000;

    public static Properties buildProducerProperties(AppConfig config) {
        var props = new Properties();
        props.putAll(config.adminClientProperties());
        config.schemaRegistryConfig().forEach(props::put);
        props.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName()
        );
        props.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            KafkaAvroSerializer.class.getName()
        );
        return props;
    }

    public static Order generateOrder(Random random) {
        String customerId = CUSTOMER_IDS[random.nextInt(CUSTOMER_IDS.length)];
        String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
        double amount =
            Math.round(
                (MIN_AMOUNT + random.nextDouble() * (MAX_AMOUNT - MIN_AMOUNT)) *
                    100.0
            ) /
            100.0;
        String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);
        long timestamp = System.currentTimeMillis();

        return new Order(orderId, customerId, amount, timestamp, category);
    }

    public static void main(String[] args) {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        AppConfig config = new AppConfig(dotenv);

        log.info("Starting sample order producer in {} mode", config.getMode());
        log.info(
            "Bootstrap servers: {}",
            config.adminClientProperties().get("bootstrap.servers")
        );
        log.info(
            "Schema Registry URL: {}",
            config.schemaRegistryConfig().get("schema.registry.url")
        );
        log.info("Target topic: {}", config.getInputTopic());

        CustomerSegmentationApp.createTopics(config);

        var props = buildProducerProperties(config);
        var random = new Random();
        var running = new AtomicBoolean(true);

        Runtime.getRuntime().addShutdownHook(
            new Thread(() -> {
                log.info("Shutting down producer...");
                running.set(false);
            })
        );

        try (var producer = new KafkaProducer<String, Order>(props)) {
            while (running.get()) {
                Order order = generateOrder(random);
                producer.send(
                    new ProducerRecord<>(
                        config.getInputTopic(),
                        order.getCustomerId(),
                        order
                    ),
                    (metadata, exception) -> {
                        if (exception != null) {
                            log.error(
                                "Failed to send order: {}",
                                exception.getMessage()
                            );
                        } else {
                            log.info(
                                "Sent order: customer={}, amount=${}, category={}, offset={}",
                                order.getCustomerId(),
                                order.getAmount(),
                                order.getCategory(),
                                metadata.offset()
                            );
                        }
                    }
                );

                try {
                    Thread.sleep(SEND_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.info("Producer stopped");
    }
}
