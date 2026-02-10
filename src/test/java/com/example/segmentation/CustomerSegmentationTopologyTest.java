package com.example.segmentation;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.segmentation.avro.CustomerSegment;
import com.example.segmentation.avro.Order;
import com.example.segmentation.model.CustomerStats;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CustomerSegmentationTopologyTest {

    private static final String MOCK_SCHEMA_REGISTRY_URL =
        "mock://test-topology";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;
    private TestOutputTopic<String, CustomerSegment> outputTopic;

    @BeforeEach
    void setUp() {
        var srConfig = Map.of("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);

        Topology topology = CustomerSegmentationTopology.build(srConfig);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName()
        );
        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName()
        );

        testDriver = new TopologyTestDriver(topology, props);

        var orderSerde = new SpecificAvroSerde<Order>();
        orderSerde.configure(srConfig, false);

        var segmentSerde = new SpecificAvroSerde<CustomerSegment>();
        segmentSerde.configure(srConfig, false);

        inputTopic = testDriver.createInputTopic(
            CustomerSegmentationTopology.INPUT_TOPIC,
            new StringSerializer(),
            orderSerde.serializer()
        );

        outputTopic = testDriver.createOutputTopic(
            CustomerSegmentationTopology.OUTPUT_TOPIC,
            new StringDeserializer(),
            segmentSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void singleOrderProducesNewSegment() {
        inputTopic.pipeInput(
            "order-1",
            new Order("order-1", "cust-1", 50.0, 1000L, "books")
        );

        KeyValue<String, CustomerSegment> result = outputTopic.readKeyValue();
        assertThat(result.key).isEqualTo("cust-1");
        assertThat(result.value.getCustomerSegment()).isEqualTo("New");
        assertThat(result.value.getPurchaseBehavior()).isEqualTo(
            "Single-Category"
        );
        assertThat(result.value.getOrderCount()).isEqualTo(1);
        assertThat(result.value.getTotalSpent()).isEqualTo(50.0);
    }

    @Test
    void multipleOrdersBuildUpToVip() {
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
            inputTopic.pipeInput(
                "order-" + i,
                new Order(
                    "order-" + i,
                    "cust-1",
                    100.0,
                    1000L * (i + 1),
                    categories[i]
                )
            );
        }

        KeyValue<String, CustomerSegment> last = null;
        while (!outputTopic.isEmpty()) {
            last = outputTopic.readKeyValue();
        }

        assertThat(last).isNotNull();
        assertThat(last.key).isEqualTo("cust-1");
        assertThat(last.value.getCustomerSegment()).isEqualTo("VIP");
        assertThat(last.value.getTotalSpent()).isEqualTo(1000.0);
        assertThat(last.value.getOrderCount()).isEqualTo(10);
        assertThat(last.value.getPurchaseBehavior()).isEqualTo("Diverse");
    }

    @Test
    void premiumBySpendThreshold() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-2", 500.0, 1000L, "jewelry")
        );

        KeyValue<String, CustomerSegment> result = outputTopic.readKeyValue();
        assertThat(result.value.getCustomerSegment()).isEqualTo("Premium");
    }

    @Test
    void regularByOrderCount() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-3", 10.0, 1000L, "books")
        );
        inputTopic.pipeInput(
            "o2",
            new Order("o2", "cust-3", 10.0, 2000L, "books")
        );

        outputTopic.readKeyValue(); // skip first
        KeyValue<String, CustomerSegment> result = outputTopic.readKeyValue();
        assertThat(result.value.getCustomerSegment()).isEqualTo("Regular");
        assertThat(result.value.getOrderCount()).isEqualTo(2);
    }

    @Test
    void multipleCustomersAreIndependent() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-A", 600.0, 1000L, "electronics")
        );
        inputTopic.pipeInput(
            "o2",
            new Order("o2", "cust-B", 30.0, 2000L, "books")
        );

        KeyValue<String, CustomerSegment> first = outputTopic.readKeyValue();
        KeyValue<String, CustomerSegment> second = outputTopic.readKeyValue();

        assertThat(first.key).isEqualTo("cust-A");
        assertThat(first.value.getCustomerSegment()).isEqualTo("Premium");
        assertThat(second.key).isEqualTo("cust-B");
        assertThat(second.value.getCustomerSegment()).isEqualTo("New");
    }

    @Test
    void stateStoreContainsAggregatedStats() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-1", 100.0, 1000L, "books")
        );
        inputTopic.pipeInput(
            "o2",
            new Order("o2", "cust-1", 200.0, 2000L, "electronics")
        );

        KeyValueStore<String, CustomerStats> store =
            testDriver.getKeyValueStore(
                CustomerSegmentationTopology.STATS_STORE
            );

        CustomerStats stats = store.get("cust-1");
        assertThat(stats).isNotNull();
        assertThat(stats.getTotalSpent()).isEqualTo(300.0);
        assertThat(stats.getOrderCount()).isEqualTo(2);
        assertThat(stats.getCategoriesCount()).isEqualTo(2);
    }

    @Test
    void avgOrderValueIsCorrect() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-1", 100.0, 1000L, "a")
        );
        inputTopic.pipeInput(
            "o2",
            new Order("o2", "cust-1", 300.0, 2000L, "b")
        );

        outputTopic.readKeyValue(); // skip first
        KeyValue<String, CustomerSegment> result = outputTopic.readKeyValue();
        assertThat(result.value.getAvgOrderValue()).isEqualTo(200.0);
    }

    @Test
    void categoriesPurchasedCountInOutput() {
        inputTopic.pipeInput(
            "o1",
            new Order("o1", "cust-1", 10.0, 1000L, "books")
        );
        inputTopic.pipeInput(
            "o2",
            new Order("o2", "cust-1", 20.0, 2000L, "electronics")
        );
        inputTopic.pipeInput(
            "o3",
            new Order("o3", "cust-1", 30.0, 3000L, "books")
        ); // duplicate category

        KeyValue<String, CustomerSegment> last = null;
        while (!outputTopic.isEmpty()) {
            last = outputTopic.readKeyValue();
        }

        assertThat(last).isNotNull();
        assertThat(last.value.getCategoriesPurchased()).isEqualTo(2);
        assertThat(last.value.getPurchaseBehavior()).isEqualTo(
            "Multi-Category"
        );
    }

    @Test
    void avroSerializationRoundTripThroughTopology() {
        Order order = new Order("rt-1", "cust-rt", 250.0, 5000L, "sports");
        inputTopic.pipeInput("rt-1", order);

        KeyValue<String, CustomerSegment> result = outputTopic.readKeyValue();
        assertThat(result.key).isEqualTo("cust-rt");
        assertThat(result.value.getCustomerId()).isEqualTo("cust-rt");
        assertThat(result.value.getTotalSpent()).isEqualTo(250.0);
        assertThat(result.value.getOrderCount()).isEqualTo(1);
        assertThat(result.value.getAvgOrderValue()).isEqualTo(250.0);
        assertThat(result.value.getCategoriesPurchased()).isEqualTo(1);
        assertThat(result.value.getCustomerSegment()).isEqualTo("Regular");
        assertThat(result.value.getPurchaseBehavior()).isEqualTo(
            "Single-Category"
        );
    }
}
